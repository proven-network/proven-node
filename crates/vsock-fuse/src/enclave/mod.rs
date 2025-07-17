//! Enclave-side VSOCK-FUSE implementation
//!
//! This module contains all the components that run inside the secure enclave,
//! including the FUSE filesystem, encryption, and metadata management.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    BlobId, TierHint,
    config::Config,
    encryption::{EncryptionLayer, MasterKey},
    error::Result,
    metadata::{LocalEncryptedMetadataStore, MetadataStorage, SnapshotManager},
    storage::BlobStorage,
};

pub mod client;
pub mod fuse;

/// Handle for a mounted FUSE filesystem that unmounts on drop
pub struct FuseHandle {
    mountpoint: PathBuf,
}

impl Drop for FuseHandle {
    fn drop(&mut self) {
        tracing::info!("Unmounting FUSE filesystem at {:?}", self.mountpoint);

        // Unmount the filesystem
        #[cfg(target_os = "macos")]
        {
            let _ = std::process::Command::new("umount")
                .arg(&self.mountpoint)
                .output();
        }
        #[cfg(target_os = "linux")]
        {
            let _ = std::process::Command::new("fusermount")
                .arg("-u")
                .arg(&self.mountpoint)
                .output();
        }

        // Give the FUSE thread a moment to shutdown
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

pub use client::EnclaveStorageClient;
pub use fuse::VsockFuseFs;

/// Enclave-side VSOCK-FUSE service
pub struct EnclaveService {
    /// Configuration
    config: Arc<Config>,
    /// Encryption layer
    encryption: Arc<EncryptionLayer>,
    /// Metadata store
    metadata: Arc<LocalEncryptedMetadataStore>,
    /// Storage client
    storage_client: Arc<EnclaveStorageClient>,
}

impl EnclaveService {
    /// Create a new enclave service
    pub async fn new(config: Config, master_key: MasterKey) -> Result<Self> {
        // Initialize encryption layer
        let encryption = Arc::new(EncryptionLayer::new(
            master_key,
            config.filesystem.block_size,
        )?);

        // Create storage client
        let storage_client =
            EnclaveStorageClient::connect(config.rpc.host_cid, config.rpc.port).await?;
        let storage_client = Arc::new(storage_client);

        // Create metadata storage adapter for remote sync (optional)
        let metadata_storage = Arc::new(MetadataStorageAdapter::new(storage_client.clone()));

        // Create journal directory if it doesn't exist
        let journal_dir = std::path::Path::new("/var/lib/vsock-fuse/journal");
        std::fs::create_dir_all(journal_dir).ok();

        // Initialize local metadata store with journaling
        let metadata = if journal_dir.exists() && journal_dir.is_dir() {
            // Use journaling for durability
            Arc::new(LocalEncryptedMetadataStore::with_journal(
                encryption.clone(),
                Some(metadata_storage), // For future durability sync
                journal_dir,
            )?)
        } else {
            // Fall back to in-memory only if journal dir not available
            tracing::warn!(
                "Journal directory not available, metadata will not persist across restarts"
            );
            Arc::new(LocalEncryptedMetadataStore::new(
                encryption.clone(),
                Some(metadata_storage),
            ))
        };

        Ok(Self {
            config: Arc::new(config),
            encryption,
            metadata,
            storage_client,
        })
    }

    /// Mount the FUSE filesystem
    /// Returns a handle that can be used to unmount the filesystem
    ///
    /// # Requirements
    /// This must be called from within a tokio runtime with multiple worker threads
    /// to avoid deadlocks. The FUSE callbacks run on OS threads and use blocking
    /// operations to wait for async responses.
    pub fn mount(&self, mountpoint: &Path) -> Result<FuseHandle> {
        // Create FUSE filesystem with channel
        let (fs, rx) = VsockFuseFs::new(
            self.config.clone(),
            self.encryption.clone(),
            self.metadata.clone(),
            self.storage_client.clone(),
        );

        // Start the async handler in the runtime
        let config = self.config.clone();
        let crypto = self.encryption.clone();
        let metadata = self.metadata.clone();
        let storage = self.storage_client.clone();

        // Get current runtime handle to spawn the async handler
        let runtime_handle = tokio::runtime::Handle::current();
        runtime_handle.spawn(async move {
            let handler = Arc::new(crate::fuse_async::FuseAsyncHandler::new(
                metadata,
                storage,
                crypto,
                config.filesystem.block_size,
            ));
            handler.run(rx).await;
        });

        // Set up FUSE options
        let options = vec![
            fuser::MountOption::FSName("vsock-fuse".to_string()),
            fuser::MountOption::AutoUnmount,
            // Note: AllowOther requires user_allow_other in /etc/fuse.conf on Linux
            // On macOS, it may require special permissions
            // Commenting out for now to avoid permission issues
            // fuser::MountOption::AllowOther,
        ];

        // Clone mountpoint for the handle and thread
        let mountpoint_clone = mountpoint.to_path_buf();
        let mountpoint_thread = mountpoint.to_path_buf();

        // Spawn mount in a separate thread since it's blocking
        std::thread::spawn(move || {
            tracing::info!("Starting FUSE mount2 call to {:?}", mountpoint_thread);
            match fuser::mount2(fs, &mountpoint_thread, &options) {
                Ok(()) => {
                    tracing::info!("FUSE mount2 returned successfully");
                }
                Err(e) => {
                    tracing::error!("FUSE mount error: {}", e);
                }
            }
        });

        // Give mount a moment to initialize
        tracing::info!("Waiting for mount to initialize...");
        std::thread::sleep(std::time::Duration::from_millis(500));

        Ok(FuseHandle {
            mountpoint: mountpoint_clone,
        })
    }

    /// Get a handle to the metadata store
    pub fn metadata(&self) -> Arc<LocalEncryptedMetadataStore> {
        self.metadata.clone()
    }

    /// Get a handle to the encryption layer
    pub fn encryption(&self) -> Arc<EncryptionLayer> {
        self.encryption.clone()
    }

    /// Start periodic snapshot creation
    pub fn start_snapshot_task(&self, journal_dir: &Path, interval: std::time::Duration) {
        let metadata = self.metadata.clone();
        let journal_path = journal_dir.to_path_buf();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            let mut sequence: u64 = 0;

            loop {
                interval_timer.tick().await;

                // Get current sequence from journal or use counter
                sequence += 1;

                // Create snapshot manager
                if let Some(local_store) = metadata.get_local_store() {
                    let snapshot_manager = SnapshotManager::new(&journal_path, local_store);

                    match snapshot_manager.create_snapshot(sequence) {
                        Ok(path) => {
                            tracing::info!("Created snapshot at {:?}", path);

                            // Clean up old snapshots, keep last 3
                            if let Err(e) = snapshot_manager.cleanup_old_snapshots(3) {
                                tracing::warn!("Failed to cleanup old snapshots: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to create snapshot: {}", e);
                        }
                    }
                }
            }
        });
    }
}

/// Builder for enclave service
#[derive(Default)]
pub struct EnclaveServiceBuilder {
    config: Option<Config>,
    master_key: Option<MasterKey>,
}

impl EnclaveServiceBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: None,
            master_key: None,
        }
    }

    /// Set the configuration
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the master key
    pub fn master_key(mut self, key: MasterKey) -> Self {
        self.master_key = Some(key);
        self
    }

    /// Derive master key from attestation
    pub fn derive_master_key(
        mut self,
        attestation: &[u8],
        user_seed: Option<&[u8]>,
    ) -> Result<Self> {
        use crate::encryption::KeyManager;
        let key = KeyManager::derive_from_attestation(attestation, user_seed)?;
        self.master_key = Some(key);
        Ok(self)
    }

    /// Build the enclave service
    pub async fn build(self) -> Result<EnclaveService> {
        let config = self
            .config
            .ok_or_else(|| crate::error::VsockFuseError::Configuration {
                message: "Configuration not set".to_string(),
            })?;

        let master_key =
            self.master_key
                .ok_or_else(|| crate::error::VsockFuseError::Configuration {
                    message: "Master key not set".to_string(),
                })?;

        EnclaveService::new(config, master_key).await
    }
}

impl std::fmt::Debug for EnclaveService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnclaveService")
            .field("config", &self.config)
            .finish_non_exhaustive() // Exclude master key
    }
}

/// Adapter to convert BlobStorage to MetadataStorage
struct MetadataStorageAdapter {
    storage: Arc<dyn BlobStorage>,
}

impl MetadataStorageAdapter {
    fn new(storage: Arc<dyn BlobStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait::async_trait]
impl MetadataStorage for MetadataStorageAdapter {
    async fn store_blob(&self, blob_id: BlobId, data: Vec<u8>) -> Result<()> {
        self.storage
            .store_blob(blob_id, data, TierHint::PreferHot)
            .await
    }

    async fn get_blob(&self, blob_id: BlobId) -> Result<Vec<u8>> {
        let blob_data = self.storage.get_blob(blob_id).await?;
        Ok(blob_data.data)
    }

    async fn delete_blob(&self, blob_id: BlobId) -> Result<()> {
        self.storage.delete_blob(blob_id).await
    }

    async fn list_blobs(&self, prefix: Option<&[u8]>) -> Result<Vec<BlobId>> {
        let blobs = self.storage.list_blobs(prefix).await?;
        Ok(blobs.into_iter().map(|info| info.blob_id).collect())
    }
}
