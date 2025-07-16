//! Local encrypted metadata store that keeps metadata in the enclave
//!
//! This implementation stores all metadata locally in the enclave and only
//! syncs to the host for durability. Directory structures and file metadata
//! are kept in memory for fast access without RPC round trips.

use std::sync::Arc;
use tracing::{debug, info};

use crate::{
    DirectoryId, FileId, FileMetadata,
    encryption::EncryptionLayer,
    error::{Result, VsockFuseError},
    metadata::{DirectoryEntry, LocalMetadataStore},
};

/// Encrypted metadata store with local storage
pub struct LocalEncryptedMetadataStore {
    /// Local metadata storage
    local_store: Arc<LocalMetadataStore>,
    /// Encryption layer for names
    crypto: Arc<EncryptionLayer>,
    /// Optional remote storage for durability
    remote_storage: Option<Arc<dyn super::MetadataStorage>>,
}

impl LocalEncryptedMetadataStore {
    /// Create a new local encrypted metadata store
    pub fn new(
        crypto: Arc<EncryptionLayer>,
        remote_storage: Option<Arc<dyn super::MetadataStorage>>,
    ) -> Self {
        let local_store = Arc::new(LocalMetadataStore::new());
        local_store.init_root();

        Self {
            local_store,
            crypto,
            remote_storage,
        }
    }

    /// Get file metadata
    pub async fn get_metadata(&self, file_id: &FileId) -> Result<Option<FileMetadata>> {
        Ok(self.local_store.get_file_metadata(file_id))
    }

    /// Save file metadata
    pub async fn save_metadata(&self, metadata: &FileMetadata) -> Result<()> {
        // Store locally
        self.local_store.put_file_metadata(metadata.clone());

        // Optionally sync to remote for durability
        if let Some(remote) = &self.remote_storage {
            // We could encrypt and store to remote here for backup
            // For now, we'll skip this to avoid the round trip
            debug!(
                "Skipping remote metadata sync for file {:?}",
                metadata.file_id
            );
        }

        Ok(())
    }

    /// Update metadata directly
    pub async fn update_metadata_direct(
        &self,
        _file_id: &FileId,
        metadata: FileMetadata,
    ) -> Result<()> {
        self.save_metadata(&metadata).await
    }

    /// Lookup entry in directory
    pub async fn lookup_entry(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
    ) -> Result<Option<DirectoryEntry>> {
        let name_str = std::str::from_utf8(name).map_err(|_| VsockFuseError::InvalidArgument {
            message: "Invalid UTF-8 in filename".to_string(),
        })?;

        // For local store, we don't encrypt names since it's all in enclave memory
        if let Some(entry) = self.local_store.find_directory_entry(parent_id, name) {
            Ok(Some(DirectoryEntry {
                name: entry.name,
                file_id: entry.file_id,
                file_type: entry.file_type,
            }))
        } else {
            Ok(None)
        }
    }

    /// List directory contents
    pub async fn list_directory(&self, dir_id: &DirectoryId) -> Result<Vec<DirectoryEntry>> {
        match self.local_store.get_directory(dir_id) {
            Some(entries) => Ok(entries
                .into_iter()
                .map(|e| DirectoryEntry {
                    name: e.name,
                    file_id: e.file_id,
                    file_type: e.file_type,
                })
                .collect()),
            None => Ok(vec![]),
        }
    }

    /// Create a file
    pub async fn create_file(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        // Save file metadata
        self.save_metadata(&metadata).await?;

        // Add to parent directory
        self.local_store.add_directory_entry(
            parent_id,
            name.to_vec(),
            metadata.file_id,
            metadata.file_type,
        )?;

        Ok(())
    }

    /// Delete a file
    pub async fn delete_file(&self, parent_id: &DirectoryId, name: &[u8]) -> Result<()> {
        // Remove from directory
        if let Some(entry) = self.local_store.remove_directory_entry(parent_id, name) {
            // Delete metadata
            self.local_store.delete_file_metadata(&entry.file_id);
        }

        Ok(())
    }

    /// Create a directory
    pub async fn create_directory(
        &self,
        parent_id: &DirectoryId,
        name: &[u8],
        metadata: FileMetadata,
    ) -> Result<()> {
        // Initialize empty directory
        let dir_id = DirectoryId(metadata.file_id);
        self.local_store.put_directory(dir_id, vec![]);

        // Save directory metadata
        self.save_metadata(&metadata).await?;

        // Add to parent
        self.local_store.add_directory_entry(
            parent_id,
            name.to_vec(),
            metadata.file_id,
            metadata.file_type,
        )?;

        Ok(())
    }

    /// Delete a directory
    pub async fn delete_directory(&self, parent_id: &DirectoryId, name: &[u8]) -> Result<()> {
        // Find directory to delete
        if let Some(entry) = self.local_store.remove_directory_entry(parent_id, name) {
            let dir_id = DirectoryId(entry.file_id);

            // Check if empty
            if let Some(entries) = self.local_store.get_directory(&dir_id)
                && !entries.is_empty()
            {
                // Re-add the entry since delete failed
                self.local_store.add_directory_entry(
                    parent_id,
                    entry.name,
                    entry.file_id,
                    entry.file_type,
                )?;

                return Err(VsockFuseError::NotEmpty {
                    path: std::path::PathBuf::from(String::from_utf8_lossy(name).to_string()),
                });
            }

            // Delete metadata
            self.local_store.delete_file_metadata(&entry.file_id);
        }

        Ok(())
    }

    /// Rename an entry
    pub async fn rename_entry(
        &self,
        old_parent: &DirectoryId,
        old_name: &[u8],
        new_parent: &DirectoryId,
        new_name: &[u8],
    ) -> Result<()> {
        // Remove from old location
        if let Some(entry) = self
            .local_store
            .remove_directory_entry(old_parent, old_name)
        {
            // Add to new location
            if let Err(e) = self.local_store.add_directory_entry(
                new_parent,
                new_name.to_vec(),
                entry.file_id,
                entry.file_type,
            ) {
                // Restore old entry on failure
                let _ = self.local_store.add_directory_entry(
                    old_parent,
                    entry.name,
                    entry.file_id,
                    entry.file_type,
                );
                return Err(e);
            }

            // U&&ate parent in metadata if moving directories
            if old_parent != new_parent
                && let Some(mut metadata) = self.local_store.get_file_metadata(&entry.file_id)
            {
                metadata.parent_id = Some(*new_parent);
                self.save_metadata(&metadata).await?;
            }
        }

        Ok(())
    }

    /// Get storage statistics
    pub fn get_local_stats(&self) -> super::LocalMetadataStats {
        self.local_store.get_stats()
    }

    /// Get storage statistics (async wrapper for consistency)
    pub async fn get_stats(&self) -> super::LocalMetadataStats {
        self.get_local_stats()
    }

    /// Sync all metadata to remote storage (for durability)
    pub async fn sync_to_remote(&self) -> Result<()> {
        if let Some(remote) = &self.remote_storage {
            info!("Syncing metadata to remote storage");
            // TODO: Implement remote sync
            // This would encrypt and batch upload all metadata
        }
        Ok(())
    }

    /// Load metadata from remote storage (for recovery)
    pub async fn load_from_remote(&self) -> Result<()> {
        if let Some(remote) = &self.remote_storage {
            info!("Loading metadata from remote storage");
            // TODO: Implement remote load
            // This would download and decrypt all metadata
        }
        Ok(())
    }
}
