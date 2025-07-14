//! Factory for creating unified local storage instances
//!
//! This module provides factory implementations for creating GroupStorage
//! instances with different storage backends.

use crate::{
    ConsensusGroupId, StorageConfig,
    core::group::GroupConsensusTypeConfig,
    core::state_machine::{LocalStateMachine as StorageBackedLocalState, group::GroupStateMachine},
    error::{ConsensusResult, Error},
    operations::handlers::GroupStreamOperationResponse,
    storage_backends::{memory::MemoryStorage, rocksdb::RocksDBStorage},
};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::group_store::GroupStorage;
use super::log_storage::GroupLogStorage;

/// Components needed for group consensus
pub struct GroupConsensusComponents<L, S> {
    /// Log storage component
    pub log_storage: L,
    /// State machine component
    pub state_machine: S,
}

/// Trait for creating local storage instances
#[async_trait::async_trait]
pub trait GroupStorageFactory: Send + Sync + 'static {
    /// Log storage type that implements RaftLogStorage
    type LogStorage: openraft::storage::RaftLogStorage<GroupConsensusTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;
    /// State machine type that implements RaftStateMachine
    type StateMachine: openraft::storage::RaftStateMachine<GroupConsensusTypeConfig>
        + Send
        + Sync
        + 'static;
    /// Legacy storage type (for backward compatibility)
    type Storage: openraft::storage::RaftLogStorage<GroupConsensusTypeConfig>
        + openraft::storage::RaftStateMachine<GroupConsensusTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Create separate log storage and state machine components
    async fn create_components(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<GroupConsensusComponents<Self::LogStorage, Self::StateMachine>>;

    /// Create a new local storage instance for a consensus group (legacy)
    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage>;

    /// Create a new local storage instance with a handler-based state machine (legacy)
    async fn create_storage_with_state_machine(
        &self,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<Self::Storage>;
}

/// Factory for memory-based local storage
pub struct MemoryGroupStorageFactory {
    storage_engine: Arc<MemoryStorage>,
}

impl MemoryGroupStorageFactory {
    /// Create a new memory storage factory
    pub fn new() -> Self {
        Self {
            storage_engine: Arc::new(MemoryStorage::new()),
        }
    }
}

impl Default for MemoryGroupStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl GroupStorageFactory for MemoryGroupStorageFactory {
    type LogStorage = GroupLogStorage<MemoryStorage>;
    type StateMachine = GroupStateMachine;
    type Storage = UnifiedGroupStorage; // Use unified storage for legacy compatibility

    async fn create_components(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<GroupConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        // Create log storage
        let log_storage = GroupLogStorage::new(self.storage_engine.clone(), group_id).await?;

        // Create state machine using the existing local state
        // We need to extract the StorageBackedLocalState from the RwLock
        let storage_state_arc = {
            let state = local_state.read().await;
            // Clone the underlying StorageBackedLocalState into a new Arc
            Arc::new(state.clone())
        };
        let state_machine = GroupStateMachine::new(storage_state_arc, group_id);

        Ok(GroupConsensusComponents {
            log_storage,
            state_machine,
        })
    }

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        // For legacy compatibility, create unified storage
        let storage = GroupStorage::new(self.storage_engine.clone(), group_id, local_state).await?;
        Ok(UnifiedGroupStorage::Memory(storage))
    }

    async fn create_storage_with_state_machine(
        &self,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<Self::Storage> {
        // For legacy compatibility, create unified storage
        let storage = GroupStorage::new_with_state_machine(
            self.storage_engine.clone(),
            group_id,
            state_machine,
        )
        .await?;
        Ok(UnifiedGroupStorage::Memory(storage))
    }
}

/// Factory for RocksDB-based local storage
pub struct RocksDBGroupStorageFactory {
    storage_engine: Arc<RocksDBStorage>,
}

impl RocksDBGroupStorageFactory {
    /// Create a new RocksDB storage factory
    pub async fn new(base_path: std::path::PathBuf) -> ConsensusResult<Self> {
        let config = crate::storage_backends::rocksdb::RocksDBAdaptorConfig {
            path: base_path,
            ..Default::default()
        };
        let storage_engine = Arc::new(
            RocksDBStorage::new(config)
                .await
                .map_err(|e| Error::storage(format!("Failed to create RocksDB storage: {e}")))?,
        );
        Ok(Self { storage_engine })
    }
}

#[async_trait::async_trait]
impl GroupStorageFactory for RocksDBGroupStorageFactory {
    type LogStorage = GroupLogStorage<RocksDBStorage>;
    type StateMachine = GroupStateMachine;
    type Storage = UnifiedGroupStorage; // Use unified storage for legacy compatibility

    async fn create_components(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<GroupConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        // Create log storage
        let log_storage = GroupLogStorage::new(self.storage_engine.clone(), group_id).await?;

        // Create state machine using the existing local state
        // We need to extract the StorageBackedLocalState from the RwLock
        let storage_state_arc = {
            let state = local_state.read().await;
            // Clone the underlying StorageBackedLocalState into a new Arc
            Arc::new(state.clone())
        };
        let state_machine = GroupStateMachine::new(storage_state_arc, group_id);

        Ok(GroupConsensusComponents {
            log_storage,
            state_machine,
        })
    }

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        // For legacy compatibility, create unified storage
        let storage = GroupStorage::new(self.storage_engine.clone(), group_id, local_state).await?;
        Ok(UnifiedGroupStorage::RocksDB(storage))
    }

    async fn create_storage_with_state_machine(
        &self,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<Self::Storage> {
        // For legacy compatibility, create unified storage
        let storage = GroupStorage::new_with_state_machine(
            self.storage_engine.clone(),
            group_id,
            state_machine,
        )
        .await?;
        Ok(UnifiedGroupStorage::RocksDB(storage))
    }
}

/// Unified local storage type that can be either memory or RocksDB
pub enum UnifiedGroupStorage {
    /// Memory-backed local storage
    Memory(GroupStorage<MemoryStorage>),
    /// RocksDB-backed local storage
    RocksDB(GroupStorage<RocksDBStorage>),
}

// Implement Clone for UnifiedGroupStorage
impl Clone for UnifiedGroupStorage {
    fn clone(&self) -> Self {
        match self {
            UnifiedGroupStorage::Memory(storage) => UnifiedGroupStorage::Memory(storage.clone()),
            UnifiedGroupStorage::RocksDB(storage) => UnifiedGroupStorage::RocksDB(storage.clone()),
        }
    }
}

// Delegate all OpenRaft traits to the inner storage
impl openraft::storage::RaftLogReader<GroupConsensusTypeConfig> for UnifiedGroupStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<openraft::Entry<GroupConsensusTypeConfig>>,
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.try_get_log_entries(range).await,
            UnifiedGroupStorage::RocksDB(storage) => storage.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<GroupConsensusTypeConfig>>,
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.read_vote().await,
            UnifiedGroupStorage::RocksDB(storage) => storage.read_vote().await,
        }
    }
}

impl openraft::storage::RaftLogStorage<GroupConsensusTypeConfig> for UnifiedGroupStorage {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<GroupConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GroupConsensusTypeConfig>> {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.save_vote(vote).await,
            UnifiedGroupStorage::RocksDB(storage) => storage.save_vote(vote).await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<GroupConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GroupConsensusTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GroupConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.append(entries, callback).await,
            UnifiedGroupStorage::RocksDB(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GroupConsensusTypeConfig>> {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.truncate(log_id).await,
            UnifiedGroupStorage::RocksDB(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), openraft::StorageError<GroupConsensusTypeConfig>> {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.purge(log_id).await,
            UnifiedGroupStorage::RocksDB(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::storage::LogState<GroupConsensusTypeConfig>,
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        match self {
            UnifiedGroupStorage::Memory(storage) => storage.get_log_state().await,
            UnifiedGroupStorage::RocksDB(storage) => storage.get_log_state().await,
        }
    }
}

/// Unified snapshot builder
pub enum UnifiedLocalSnapshotBuilder {
    /// Memory-based snapshot builder
    Memory(super::group_store::LocalSnapshotBuilder<MemoryStorage>),
    /// RocksDB-based snapshot builder
    RocksDB(super::group_store::LocalSnapshotBuilder<RocksDBStorage>),
}

impl openraft::RaftSnapshotBuilder<GroupConsensusTypeConfig> for UnifiedLocalSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::storage::Snapshot<GroupConsensusTypeConfig>,
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        match self {
            UnifiedLocalSnapshotBuilder::Memory(builder) => builder.build_snapshot().await,
            UnifiedLocalSnapshotBuilder::RocksDB(builder) => builder.build_snapshot().await,
        }
    }
}

impl openraft::storage::RaftStateMachine<GroupConsensusTypeConfig> for UnifiedGroupStorage {
    type SnapshotBuilder = UnifiedLocalSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<GroupConsensusTypeConfig>>,
            openraft::StoredMembership<GroupConsensusTypeConfig>,
        ),
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        // Since GroupStorage no longer implements RaftStateMachine, we return default values
        // The actual state machine implementation is in GroupStateMachine
        Ok((None, openraft::StoredMembership::default()))
    }

    async fn apply<I>(
        &mut self,
        _entries: I,
    ) -> Result<Vec<GroupStreamOperationResponse>, openraft::StorageError<GroupConsensusTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<GroupConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Since GroupStorage no longer implements RaftStateMachine, we return empty responses
        // The actual state machine implementation is in GroupStateMachine
        Ok(vec![])
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Return a default snapshot builder
        match self {
            UnifiedGroupStorage::Memory(_) => UnifiedLocalSnapshotBuilder::Memory(
                super::group_store::LocalSnapshotBuilder::new(Arc::new(MemoryStorage::new())),
            ),
            UnifiedGroupStorage::RocksDB(_) => {
                // Create a temporary RocksDB storage for snapshot building
                let config = crate::storage_backends::rocksdb::RocksDBAdaptorConfig {
                    path: std::env::temp_dir().join("raft_snapshot_temp"),
                    ..Default::default()
                };
                let storage = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        RocksDBStorage::new(config)
                            .await
                            .expect("Failed to create RocksDB for snapshot")
                    })
                });
                UnifiedLocalSnapshotBuilder::RocksDB(super::group_store::LocalSnapshotBuilder::new(
                    Arc::new(storage),
                ))
            }
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, openraft::StorageError<GroupConsensusTypeConfig>> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &openraft::SnapshotMeta<GroupConsensusTypeConfig>,
        _snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), openraft::StorageError<GroupConsensusTypeConfig>> {
        // Since GroupStorage no longer implements RaftStateMachine, this is a no-op
        // The actual state machine implementation is in GroupStateMachine
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<GroupConsensusTypeConfig>>,
        openraft::StorageError<GroupConsensusTypeConfig>,
    > {
        // Since GroupStorage no longer implements RaftStateMachine, return None
        // The actual state machine implementation is in GroupStateMachine
        Ok(None)
    }
}

/// Factory that creates unified local storage based on configuration
pub struct UnifiedGroupStorageFactory {
    config: StorageConfig,
}

impl UnifiedGroupStorageFactory {
    /// Create a new unified factory
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl GroupStorageFactory for UnifiedGroupStorageFactory {
    type LogStorage = UnifiedGroupStorage; // UnifiedGroupStorage implements RaftLogStorage
    type StateMachine = GroupStateMachine;
    type Storage = UnifiedGroupStorage;

    async fn create_components(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<GroupConsensusComponents<Self::LogStorage, Self::StateMachine>> {
        // Create the unified storage which handles log storage
        let log_storage = match &self.config {
            StorageConfig::Memory => {
                let factory = MemoryGroupStorageFactory::new();
                factory
                    .create_local_storage(group_id, local_state.clone())
                    .await?
            }
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBGroupStorageFactory::new(path.join("raft")).await?;
                factory
                    .create_local_storage(group_id, local_state.clone())
                    .await?
            }
        };

        // Create state machine using the existing local state
        let storage_state_arc = {
            let state = local_state.read().await;
            Arc::new(state.clone())
        };
        let state_machine = GroupStateMachine::new(storage_state_arc, group_id);

        Ok(GroupConsensusComponents {
            log_storage,
            state_machine,
        })
    }

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        match &self.config {
            StorageConfig::Memory => {
                let factory = MemoryGroupStorageFactory::new();
                factory.create_local_storage(group_id, local_state).await
            }
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBGroupStorageFactory::new(path.join("raft")).await?;
                factory.create_local_storage(group_id, local_state).await
            }
        }
    }

    async fn create_storage_with_state_machine(
        &self,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<Self::Storage> {
        match &self.config {
            StorageConfig::Memory => {
                let factory = MemoryGroupStorageFactory::new();
                factory
                    .create_storage_with_state_machine(group_id, state_machine)
                    .await
            }
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBGroupStorageFactory::new(path.join("raft")).await?;
                factory
                    .create_storage_with_state_machine(group_id, state_machine)
                    .await
            }
        }
    }
}

/// Create a local storage factory based on configuration
pub fn create_local_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<
    Box<
        dyn GroupStorageFactory<
                Storage = UnifiedGroupStorage,
                LogStorage = UnifiedGroupStorage,
                StateMachine = GroupStateMachine,
            >,
    >,
> {
    Ok(Box::new(UnifiedGroupStorageFactory::new(config.clone())))
}
