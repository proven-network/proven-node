//! Factory for creating storage instances for individual streams
//!
//! This factory creates dedicated storage instances for each stream,
//! enabling easier migration and per-stream configuration.

use crate::allocation::ConsensusGroupId;
use crate::error::ConsensusResult;
use crate::storage::StorageEngine;
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for stream-specific storage
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StreamPersistenceMode {
    /// In-memory storage
    Memory,
    /// File-based persistent storage
    File,
}

impl Default for StreamPersistenceMode {
    fn default() -> Self {
        Self::Memory
    }
}

/// Factory for creating per-stream storage instances
#[async_trait::async_trait]
pub trait PerStreamStorageFactory: Send + Sync + 'static {
    /// Type of storage engine created
    type Storage: StorageEngine + Clone + Send + Sync + 'static;

    /// Create a storage instance for a specific stream
    async fn create_stream_storage(
        &self,
        group_id: ConsensusGroupId,
        stream_id: &str,
        persistence_mode: &StreamPersistenceMode,
    ) -> ConsensusResult<Arc<Self::Storage>>;

    /// Clean up storage for a specific stream
    fn cleanup_stream_storage(
        &self,
        group_id: ConsensusGroupId,
        stream_id: &str,
    ) -> ConsensusResult<()>;

    /// Get the storage path for a stream (if applicable)
    fn get_stream_path(&self, group_id: ConsensusGroupId, stream_id: &str) -> Option<PathBuf>;
}

/// Storage backend to use for file-based storage
#[derive(Debug, Clone)]
pub enum StreamStorageBackend {
    /// Use RocksDB for file storage
    RocksDB,
    // Future: could add other backends like LevelDB, SQLite, etc.
}

impl Default for StreamStorageBackend {
    fn default() -> Self {
        Self::RocksDB
    }
}

/// Factory that can create different storage types based on persistence mode
pub struct UnifiedPerStreamFactory {
    /// Base path for file-based storage
    base_path: Option<PathBuf>,
    /// Storage backend to use for file-based storage
    storage_backend: StreamStorageBackend,
}

impl UnifiedPerStreamFactory {
    /// Create a new unified per-stream factory
    pub fn new(base_path: Option<PathBuf>) -> ConsensusResult<Self> {
        Self::with_backend(base_path, StreamStorageBackend::default())
    }

    /// Create a new unified per-stream factory with a specific storage backend
    pub fn with_backend(
        base_path: Option<PathBuf>,
        storage_backend: StreamStorageBackend,
    ) -> ConsensusResult<Self> {
        if let Some(path) = &base_path {
            std::fs::create_dir_all(path).map_err(|_e| {
                crate::error::Error::Storage(crate::error::StorageError::OperationFailed {
                    operation: "create_dir_all".to_string(),
                })
            })?;
        }
        Ok(Self {
            base_path,
            storage_backend,
        })
    }

    /// Get the base path for a specific stream
    fn stream_base_path(&self, group_id: ConsensusGroupId, stream_id: &str) -> Option<PathBuf> {
        self.base_path.as_ref().map(|base| {
            base.join(format!("group_{}", group_id.0))
                .join(format!("stream_{}", stream_id))
        })
    }
}

#[async_trait::async_trait]
impl PerStreamStorageFactory for UnifiedPerStreamFactory {
    type Storage = super::unified::UnifiedStreamStorage;

    async fn create_stream_storage(
        &self,
        group_id: ConsensusGroupId,
        stream_id: &str,
        persistence_mode: &StreamPersistenceMode,
    ) -> ConsensusResult<Arc<Self::Storage>> {
        match persistence_mode {
            StreamPersistenceMode::Memory => {
                // Create a new memory storage instance for this stream
                Ok(Arc::new(super::unified::UnifiedStreamStorage::memory()))
            }
            StreamPersistenceMode::File => {
                // Create file-based storage with the configured backend
                let path = self.stream_base_path(group_id, stream_id).ok_or_else(|| {
                    crate::error::Error::Storage(crate::error::StorageError::OperationFailed {
                        operation: "stream_base_path".to_string(),
                    })
                })?;

                match self.storage_backend {
                    StreamStorageBackend::RocksDB => {
                        // Create RocksDB storage with stream-specific path
                        let config =
                            crate::storage::adaptors::rocksdb::RocksDBAdaptorConfig::development(
                                path,
                            );
                        let storage = super::unified::UnifiedStreamStorage::rocksdb(config)
                            .await
                            .map_err(|_e| {
                                crate::error::Error::Storage(
                                    crate::error::StorageError::OperationFailed {
                                        operation: "create_rocksdb_storage".to_string(),
                                    },
                                )
                            })?;
                        Ok(Arc::new(storage))
                    }
                }
            }
        }
    }

    fn cleanup_stream_storage(
        &self,
        group_id: ConsensusGroupId,
        stream_id: &str,
    ) -> ConsensusResult<()> {
        if let Some(path) = self.stream_base_path(group_id, stream_id) {
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(|_e| {
                    crate::error::Error::Storage(crate::error::StorageError::OperationFailed {
                        operation: "cleanup_stream_storage".to_string(),
                    })
                })?;
            }
        }
        Ok(())
    }

    fn get_stream_path(&self, group_id: ConsensusGroupId, stream_id: &str) -> Option<PathBuf> {
        self.stream_base_path(group_id, stream_id)
    }
}
