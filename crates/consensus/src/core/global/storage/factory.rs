//! Factory for creating global consensus storage instances
//!
//! This module provides a factory pattern for creating global consensus storage instances
//! with different backends (memory, RocksDB, etc.) using the unified storage adaptors.

use std::sync::Arc;

use crate::config::StorageConfig;
use crate::core::global::{GlobalConsensusTypeConfig, global_state::GlobalState};
use crate::core::state_machine::global::GlobalStateMachine;
use crate::error::ConsensusResult;
use crate::storage_backends::{memory::MemoryStorage, rocksdb::RocksDBStorage};

use super::log_storage::GlobalLogStorage;
use super::log_store::GlobalStorage;

/// Components needed for global consensus
pub struct GlobalConsensusComponents<L, S> {
    /// Log storage component
    pub log_storage: L,
    /// State machine component
    pub state_machine: S,
}

/// Trait for creating global consensus storage instances
#[async_trait::async_trait]
pub trait GlobalStorageFactory: Send + Sync + 'static {
    /// Log storage type that implements RaftLogStorage
    type LogStorage: openraft::storage::RaftLogStorage<GlobalConsensusTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;
    /// State machine type that implements RaftStateMachine
    type StateMachine: openraft::storage::RaftStateMachine<GlobalConsensusTypeConfig>
        + Send
        + Sync
        + 'static;

    /// Create separate log storage and state machine components
    async fn create_components(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<GlobalConsensusComponents<Self::LogStorage, Self::StateMachine>>;

    /// Legacy method - create combined storage (deprecated)
    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<super::GlobalStorageType> {
        // Default implementation for backward compatibility
        // This will be removed once all callers are updated
        unimplemented!("Use create_components instead")
    }
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
    type LogStorage = GlobalLogStorage<MemoryStorage>;
    type StateMachine = GlobalStateMachine;

    async fn create_components(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<GlobalConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        let storage_engine = Arc::new(MemoryStorage::new());

        // Create log storage
        let log_storage = GlobalLogStorage::new(storage_engine.clone())
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        // Create state machine
        let state_machine = GlobalStateMachine::new(global_state);

        Ok(GlobalConsensusComponents {
            log_storage,
            state_machine,
        })
    }

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<super::GlobalStorageType> {
        let storage_engine = Arc::new(MemoryStorage::new());
        let unified_storage = GlobalStorage::new(storage_engine, global_state)
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        Ok(super::GlobalStorageType::MemoryBacked(unified_storage))
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
    type LogStorage = GlobalLogStorage<RocksDBStorage>;
    type StateMachine = GlobalStateMachine;

    async fn create_components(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<GlobalConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        let config =
            crate::storage_backends::rocksdb::RocksDBAdaptorConfig::development(&self.db_path);
        let storage_engine = Arc::new(
            RocksDBStorage::new(config)
                .await
                .map_err(|e| crate::error::Error::storage(e.to_string()))?,
        );

        // Create log storage
        let log_storage = GlobalLogStorage::new(storage_engine.clone())
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        // Create state machine
        let state_machine = GlobalStateMachine::new(global_state);

        Ok(GlobalConsensusComponents {
            log_storage,
            state_machine,
        })
    }

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<super::GlobalStorageType> {
        let config =
            crate::storage_backends::rocksdb::RocksDBAdaptorConfig::development(&self.db_path);
        let storage_engine = Arc::new(
            RocksDBStorage::new(config)
                .await
                .map_err(|e| crate::error::Error::storage(e.to_string()))?,
        );

        let unified_storage = GlobalStorage::new(storage_engine, global_state)
            .await
            .map_err(|e| crate::error::Error::storage(e.to_string()))?;

        Ok(super::GlobalStorageType::RocksDBBacked(unified_storage))
    }
}

/// Enum wrapper for different global log storage types
#[derive(Clone)]
pub enum GlobalLogStorageType {
    /// Memory-backed log storage
    MemoryBacked(GlobalLogStorage<MemoryStorage>),
    /// RocksDB-backed log storage
    RocksDBBacked(GlobalLogStorage<RocksDBStorage>),
}

impl std::fmt::Debug for GlobalLogStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GlobalLogStorageType::MemoryBacked(_) => {
                write!(f, "GlobalLogStorageType::MemoryBacked")
            }
            GlobalLogStorageType::RocksDBBacked(_) => {
                write!(f, "GlobalLogStorageType::RocksDBBacked")
            }
        }
    }
}

// Implement RaftLogReader for GlobalLogStorageType
impl openraft::RaftLogReader<GlobalConsensusTypeConfig> for GlobalLogStorageType {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<openraft::Entry<GlobalConsensusTypeConfig>>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.try_get_log_entries(range).await,
            GlobalLogStorageType::RocksDBBacked(storage) => {
                storage.try_get_log_entries(range).await
            }
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<GlobalConsensusTypeConfig>>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.read_vote().await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.read_vote().await,
        }
    }
}

// Implement RaftLogStorage for GlobalLogStorageType
impl openraft::storage::RaftLogStorage<GlobalConsensusTypeConfig> for GlobalLogStorageType {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.save_vote(vote).await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.save_vote(vote).await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GlobalConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.append(entries, callback).await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.truncate(log_id).await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.purge(log_id).await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<GlobalConsensusTypeConfig>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalLogStorageType::MemoryBacked(storage) => storage.get_log_state().await,
            GlobalLogStorageType::RocksDBBacked(storage) => storage.get_log_state().await,
        }
    }
}

/// Enum wrapper for different global storage types (deprecated)
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
impl openraft::RaftLogReader<GlobalConsensusTypeConfig> for GlobalStorageType {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<openraft::Entry<GlobalConsensusTypeConfig>>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.try_get_log_entries(range).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<GlobalConsensusTypeConfig>>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.read_vote().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.read_vote().await,
        }
    }
}

impl openraft::storage::RaftLogStorage<GlobalConsensusTypeConfig> for GlobalStorageType {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
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
        callback: openraft::storage::IOFlushed<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GlobalConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.append(entries, callback).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.truncate(log_id).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<GlobalConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.purge(log_id).await,
            GlobalStorageType::RocksDBBacked(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<GlobalConsensusTypeConfig>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
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
    MemoryBacked(super::log_store::UnifiedSnapshotBuilder<MemoryStorage>),
    /// RocksDB-backed snapshot builder
    RocksDBBacked(super::log_store::UnifiedSnapshotBuilder<RocksDBStorage>),
}

impl openraft::RaftSnapshotBuilder<GlobalConsensusTypeConfig> for GlobalSnapshotBuilderType {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::storage::Snapshot<GlobalConsensusTypeConfig>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalSnapshotBuilderType::MemoryBacked(builder) => builder.build_snapshot().await,
            GlobalSnapshotBuilderType::RocksDBBacked(builder) => builder.build_snapshot().await,
        }
    }
}

// RaftStateMachine implementation removed - now handled by GlobalStateMachine
// GlobalStorageType is deprecated and will be removed once all callers are updated
/*
impl openraft::storage::RaftStateMachine<GlobalConsensusTypeConfig> for GlobalStorageType {
    type SnapshotBuilder = GlobalSnapshotBuilderType;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<GlobalConsensusTypeConfig>>,
            openraft::StoredMembership<GlobalConsensusTypeConfig>,
        ),
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.applied_state().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.applied_state().await,
        }
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<
        Vec<crate::operations::handlers::GlobalOperationResponse>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    >
    where
        I: IntoIterator<Item = openraft::Entry<GlobalConsensusTypeConfig>> + Send,
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
    ) -> Result<std::io::Cursor<Vec<u8>>, openraft::StorageError<GlobalConsensusTypeConfig>> {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.begin_receiving_snapshot().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.begin_receiving_snapshot().await,
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<GlobalConsensusTypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), openraft::StorageError<GlobalConsensusTypeConfig>> {
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
        Option<openraft::storage::Snapshot<GlobalConsensusTypeConfig>>,
        openraft::StorageError<GlobalConsensusTypeConfig>,
    > {
        match self {
            GlobalStorageType::MemoryBacked(storage) => storage.get_current_snapshot().await,
            GlobalStorageType::RocksDBBacked(storage) => storage.get_current_snapshot().await,
        }
    }
}
*/

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
    type LogStorage = GlobalLogStorageType;
    type StateMachine = GlobalStateMachine;

    async fn create_components(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<GlobalConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        match &self.config {
            StorageConfig::Memory => {
                let storage_engine = Arc::new(MemoryStorage::new());

                // Create log storage
                let log_storage = GlobalLogStorage::new(storage_engine.clone())
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                // Create state machine
                let state_machine = GlobalStateMachine::new(global_state);

                Ok(GlobalConsensusComponents {
                    log_storage: GlobalLogStorageType::MemoryBacked(log_storage),
                    state_machine,
                })
            }
            StorageConfig::RocksDB { path } => {
                let config =
                    crate::storage_backends::rocksdb::RocksDBAdaptorConfig::development(path);
                let storage_engine = Arc::new(
                    RocksDBStorage::new(config)
                        .await
                        .map_err(|e| crate::error::Error::storage(e.to_string()))?,
                );

                // Create log storage
                let log_storage = GlobalLogStorage::new(storage_engine.clone())
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                // Create state machine
                let state_machine = GlobalStateMachine::new(global_state);

                Ok(GlobalConsensusComponents {
                    log_storage: GlobalLogStorageType::RocksDBBacked(log_storage),
                    state_machine,
                })
            }
        }
    }

    async fn create_storage(
        &self,
        global_state: Arc<GlobalState>,
    ) -> ConsensusResult<super::GlobalStorageType> {
        match &self.config {
            StorageConfig::Memory => {
                let storage_engine = Arc::new(MemoryStorage::new());
                let unified_storage = GlobalStorage::new(storage_engine, global_state)
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                Ok(super::GlobalStorageType::MemoryBacked(unified_storage))
            }
            StorageConfig::RocksDB { path } => {
                let config =
                    crate::storage_backends::rocksdb::RocksDBAdaptorConfig::development(path);
                let storage_engine = Arc::new(
                    RocksDBStorage::new(config)
                        .await
                        .map_err(|e| crate::error::Error::storage(e.to_string()))?,
                );

                let unified_storage = GlobalStorage::new(storage_engine, global_state)
                    .await
                    .map_err(|e| crate::error::Error::storage(e.to_string()))?;

                Ok(super::GlobalStorageType::RocksDBBacked(unified_storage))
            }
        }
    }
}

/// Create a global storage factory based on configuration
pub fn create_global_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<
    Box<
        dyn GlobalStorageFactory<
                LogStorage = GlobalLogStorageType,
                StateMachine = GlobalStateMachine,
            >,
    >,
> {
    Ok(Box::new(UnifiedGlobalStorageFactory::new(config.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::global::global_state::GlobalState;

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
