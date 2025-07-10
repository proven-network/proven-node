//! Factory for creating global consensus storage instances
//!
//! This module provides a factory pattern for creating global consensus storage instances
//! with different backends (memory, RocksDB, etc.) using the unified storage adaptors.

use std::sync::Arc;

use crate::config::StorageConfig;
use crate::error::ConsensusResult;
use crate::global::{GlobalTypeConfig, global_state::GlobalState};
use crate::storage::adaptors::{memory::MemoryStorage, rocksdb::RocksDBStorage};

use super::unified::GlobalStorage;

/// Trait for creating global consensus storage instances
#[async_trait::async_trait]
pub trait GlobalStorageFactory: Send + Sync + 'static {
    /// Storage type that implements the ConsensusStorage trait
    type Storage: super::ConsensusStorage;

    /// Create a new storage instance for global consensus
    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<Self::Storage>;
}

/// Factory for creating memory-based global storage instances
#[derive(Clone)]
pub struct MemoryGlobalStorageFactory;

impl Default for MemoryGlobalStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryGlobalStorageFactory {
    /// Create a new memory global storage factory
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl GlobalStorageFactory for MemoryGlobalStorageFactory {
    type Storage = GlobalStorageType;

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<Self::Storage> {
        let storage_engine = Arc::new(MemoryStorage::new());
        let unified_storage = GlobalStorage::new(storage_engine, global_state)
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        Ok(GlobalStorageType::MemoryBacked(unified_storage))
    }
}

/// Factory for creating RocksDB-based global storage instances
#[derive(Clone)]
pub struct RocksDBGlobalStorageFactory {
    /// Base path for the database
    db_path: String,
}

impl RocksDBGlobalStorageFactory {
    /// Create a new RocksDB global storage factory
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }
}

#[async_trait::async_trait]
impl GlobalStorageFactory for RocksDBGlobalStorageFactory {
    type Storage = GlobalStorageType;

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<Self::Storage> {
        let config =
            crate::storage::adaptors::rocksdb::RocksDBAdaptorConfig::development(&self.db_path);
        let storage_engine = Arc::new(
            RocksDBStorage::new(config)
                .await
                .map_err(|e| crate::error::Error::storage(e.to_string()))?,
        );

        let unified_storage = GlobalStorage::new(storage_engine, global_state)
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        Ok(GlobalStorageType::RocksDBBacked(unified_storage))
    }
}

/// Enum wrapper for different global storage types
#[derive(Clone)]
pub enum GlobalStorageType {
    /// Memory-backed storage
    MemoryBacked(GlobalStorage<MemoryStorage>),
    /// RocksDB-backed storage
    RocksDBBacked(GlobalStorage<RocksDBStorage>),
}

impl std::fmt::Debug for GlobalStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GlobalStorageType::MemoryBacked(_) => write!(f, "GlobalStorageType::MemoryBacked"),
            GlobalStorageType::RocksDBBacked(_) => write!(f, "GlobalStorageType::RocksDBBacked"),
        }
    }
}

// ConsensusStorage implementation is in mod.rs to avoid duplication

// Implement OpenRaft traits by delegating to the inner storage
impl openraft::RaftLogReader<GlobalTypeConfig> for GlobalStorageType {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<GlobalTypeConfig>>, openraft::StorageError<GlobalTypeConfig>>
    {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.try_get_log_entries(range).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<GlobalTypeConfig>>, openraft::StorageError<GlobalTypeConfig>>
    {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.read_vote().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.read_vote().await,
        }
    }
}

impl openraft::storage::RaftLogStorage<GlobalTypeConfig> for GlobalStorageType {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GlobalTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.save_vote(vote).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.save_vote(vote).await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<GlobalTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.append(entries, callback).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<GlobalTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.truncate(log_id).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<GlobalTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.purge(log_id).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<GlobalTypeConfig>,
        openraft::StorageError<GlobalTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.get_log_state().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.get_log_state().await,
        }
    }
}

/// Snapshot builder enum for different storage types
pub enum GlobalSnapshotBuilderType {
    /// Memory-backed snapshot builder
    MemoryBacked(super::unified::UnifiedSnapshotBuilder<MemoryStorage>),
    /// RocksDB-backed snapshot builder
    RocksDBBacked(super::unified::UnifiedSnapshotBuilder<RocksDBStorage>),
}

impl openraft::RaftSnapshotBuilder<GlobalTypeConfig> for GlobalSnapshotBuilderType {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::storage::Snapshot<GlobalTypeConfig>,
        openraft::StorageError<GlobalTypeConfig>,
    > {
        match self {
            GlobalSnapshotBuilderType::MemoryBacked(builder) => builder.build_snapshot().await,
            GlobalSnapshotBuilderType::RocksDBBacked(builder) => builder.build_snapshot().await,
        }
    }
}

impl openraft::storage::RaftStateMachine<GlobalTypeConfig> for GlobalStorageType {
    type SnapshotBuilder = GlobalSnapshotBuilderType;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<GlobalTypeConfig>>,
            openraft::StoredMembership<GlobalTypeConfig>,
        ),
        openraft::StorageError<GlobalTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.applied_state().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.applied_state().await,
        }
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<crate::global::GlobalResponse>, openraft::StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.apply(entries).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.apply(entries).await,
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        match self {
            GlobalStorageType::MemoryBacked(storage) => {
                GlobalSnapshotBuilderType::MemoryBacked(storage.get_snapshot_builder().await)
            }
            GlobalStorageType::RocksDBBacked(storage) => {
                GlobalSnapshotBuilderType::RocksDBBacked(storage.get_snapshot_builder().await)
            }
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, openraft::StorageError<GlobalTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.begin_receiving_snapshot().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.begin_receiving_snapshot().await,
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<GlobalTypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), openraft::StorageError<GlobalTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => {
                storage.install_snapshot(meta, snapshot).await
            }
            GlobalStorageType::RocksDBBacked(storage) => {
                storage.install_snapshot(meta, snapshot).await
            }
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<GlobalTypeConfig>>,
        openraft::StorageError<GlobalTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.get_current_snapshot().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.get_current_snapshot().await,
        }
    }
}

/// Unified global storage factory that can create either memory or RocksDB storage
pub struct UnifiedGlobalStorageFactory {
    config: StorageConfig,
}

impl UnifiedGlobalStorageFactory {
    /// Create a new unified global storage factory
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl GlobalStorageFactory for UnifiedGlobalStorageFactory {
    type Storage = GlobalStorageType;

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<Self::Storage> {
        match &self.config {
            StorageConfig::Memory => {
                let storage_engine = Arc::new(MemoryStorage::new());
                let unified_storage = GlobalStorage::new(storage_engine, global_state)
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                Ok(GlobalStorageType::MemoryBacked(unified_storage))
            }
            StorageConfig::RocksDB { path } => {
                let config =
                    crate::storage::adaptors::rocksdb::RocksDBAdaptorConfig::development(path);
                let storage_engine = Arc::new(
                    RocksDBStorage::new(config)
                        .await
                        .map_err(|e| crate::error::Error::storage(e.to_string()))?,
                );

                let unified_storage = GlobalStorage::new(storage_engine, global_state)
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                Ok(GlobalStorageType::RocksDBBacked(unified_storage))
            }
        }
    }
}

/// Create a global storage factory based on configuration
pub fn create_global_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<Box<dyn GlobalStorageFactory<Storage = GlobalStorageType>>> {
    Ok(Box::new(UnifiedGlobalStorageFactory::new(config.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::global_state::GlobalState;

    #[tokio::test]
    async fn test_memory_global_storage_factory() {
        let factory = MemoryGlobalStorageFactory::new();
        let global_state = Arc::new(GlobalState::new());

        let storage = factory.create_storage(global_state).await.unwrap();
        assert!(matches!(storage, GlobalStorageType::MemoryBacked(_)));
    }

    #[tokio::test]
    async fn test_create_global_storage_factory_from_config() {
        // Test memory config
        let memory_config = StorageConfig::Memory;
        let factory = create_global_storage_factory(&memory_config).unwrap();
        let global_state = Arc::new(GlobalState::new());

        let storage = factory.create_storage(global_state).await.unwrap();
        assert!(matches!(storage, GlobalStorageType::MemoryBacked(_)));

        // TODO: Add RocksDB test once column family setup is properly configured
    }
}
