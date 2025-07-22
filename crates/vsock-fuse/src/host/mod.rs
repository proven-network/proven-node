//! Host-side VSOCK-FUSE implementation
//!
//! This module contains all the components that run on the host system,
//! including the storage backend and RPC server.

use proven_logger::{debug, error, info};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::{config::Config, error::Result, storage::BlobStorage};

pub mod migration;
pub mod policy;
pub mod server;
pub mod storage;

pub use migration::MigrationScheduler;
pub use policy::{PolicyEngine, TieringPolicy};
pub use server::HostStorageServer;
pub use storage::{ColdTier, HotTier, TieredStorage};

/// Host-side VSOCK-FUSE service
pub struct HostService {
    /// Configuration
    config: Arc<Config>,
    /// Storage backend
    storage: Arc<TieredStorage>,
    /// RPC server
    server: Option<Arc<HostStorageServer>>,
    /// Policy engine
    _policy_engine: Arc<PolicyEngine>, // TODO: apply policy to storage
    /// Migration scheduler
    migration_scheduler: Arc<Mutex<MigrationScheduler>>,
}

impl HostService {
    /// Create a new host service
    pub async fn new(config: Config) -> Result<Self> {
        // Create policy engine first
        let policy = TieringPolicy::default(); // TODO: Load from config
        let policy_engine = Arc::new(PolicyEngine::new(policy));

        // Initialize storage backend with policy engine
        let mut storage = TieredStorage::new(
            config.storage.hot_tier.base_path.clone(),
            config.storage.cold_tier.bucket.clone(),
            config.storage.cold_tier.prefix.clone(),
        )
        .await?;
        storage.set_policy_engine(policy_engine.clone());
        let storage = Arc::new(storage);

        // RPC server will be created when started
        let server = None;

        // Create migration scheduler
        let hot_tier = Arc::new(HotTier::new(config.storage.hot_tier.base_path.clone()).await?);
        let cold_tier = Arc::new(
            ColdTier::new(
                config.storage.cold_tier.bucket.clone(),
                config.storage.cold_tier.prefix.clone(),
            )
            .await?,
        );

        let migration_scheduler = Arc::new(Mutex::new(MigrationScheduler::new(
            policy_engine.clone(),
            hot_tier,
            cold_tier,
        )));

        Ok(Self {
            config: Arc::new(config),
            storage,
            server,
            _policy_engine: policy_engine,
            migration_scheduler,
        })
    }

    /// Start the host service
    pub async fn start(&mut self) -> Result<()> {
        // Create and start the RPC server in a background task
        let server = HostStorageServer::new(self.storage.clone(), self.config.rpc.port);

        let _server_handle = tokio::spawn(async move {
            if let Err(e) = server.serve().await {
                error!("RPC server error: {e}");
            }
        });

        // Store server handle for potential future use
        self.server = Some(Arc::new(HostStorageServer::new(
            self.storage.clone(),
            self.config.rpc.port,
        )));

        // Start background tasks (migration, cleanup, etc.)
        self.start_background_tasks().await?;

        Ok(())
    }

    /// Stop the host service
    pub async fn stop(&self) -> Result<()> {
        // RPC server will stop when its task is dropped

        // Flush any pending operations
        self.storage.flush().await?;

        Ok(())
    }

    /// Start background tasks
    async fn start_background_tasks(&self) -> Result<()> {
        // Start migration scheduler
        {
            let mut scheduler = self.migration_scheduler.lock().await;
            let scan_interval = Duration::from_secs(300); // Scan every 5 minutes
            scheduler.start(scan_interval).await?;
            info!(
                "Started migration scheduler with {}s scan interval",
                scan_interval.as_secs()
            );
        }

        // Start cleanup task
        let storage = self.storage.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour
            loop {
                interval.tick().await;
                if let Err(e) = storage.flush().await {
                    error!("Failed to flush storage: {e}");
                }
                debug!("Storage cleanup completed");
            }
        });

        // Start metrics collection
        let storage = self.storage.clone();
        let migration_scheduler = self.migration_scheduler.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Every minute
            loop {
                interval.tick().await;

                // Collect storage stats
                match storage.get_stats().await {
                    Ok(stats) => {
                        info!(
                            "Storage stats - Hot tier: {}/{} bytes ({:.1}% full), Cold tier: {}/{} bytes",
                            stats.hot_tier.used_bytes,
                            stats.hot_tier.total_bytes,
                            (stats.hot_tier.used_bytes as f64 / stats.hot_tier.total_bytes as f64)
                                * 100.0,
                            stats.cold_tier.used_bytes,
                            stats.cold_tier.total_bytes
                        );
                    }
                    Err(e) => {
                        error!("Failed to get storage stats: {e}");
                    }
                }

                // Collect migration stats
                let scheduler = migration_scheduler.lock().await;
                let migration_stats = scheduler.get_stats().await;
                let queue_size = scheduler.get_queue_size().await;
                drop(scheduler);

                info!(
                    "Migration stats - Total: {}, Success: {}, Failed: {}, Queue: {}",
                    migration_stats.total_migrations,
                    migration_stats.successful_migrations,
                    migration_stats.failed_migrations,
                    queue_size
                );
            }
        });

        Ok(())
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> Result<crate::storage::StorageStats> {
        self.storage.get_stats().await
    }
}

/// Builder for host service
#[derive(Default)]
pub struct HostServiceBuilder {
    config: Option<Config>,
}

impl HostServiceBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self { config: None }
    }

    /// Set the configuration
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Load configuration from file
    pub fn config_from_file(mut self, path: &Path) -> Result<Self> {
        let config = Config::from_file(&path.to_path_buf())?;
        self.config = Some(config);
        Ok(self)
    }

    /// Build the host service
    pub async fn build(self) -> Result<HostService> {
        let config = self
            .config
            .ok_or_else(|| crate::error::VsockFuseError::Configuration {
                message: "Configuration not set".to_string(),
            })?;

        HostService::new(config).await
    }
}
