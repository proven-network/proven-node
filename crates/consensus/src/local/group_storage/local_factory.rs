//! Factory for creating unified local storage instances
//!
//! This module provides factory implementations for creating LocalStorage
//! instances with different storage backends.

use crate::{
    StorageConfig,
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error},
    local::{LocalTypeConfig, StorageBackedLocalState, group_storage::LocalStorage},
    storage::adaptors::{memory::MemoryStorage, rocksdb::RocksDBStorage},
};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Trait for creating local storage instances
#[async_trait::async_trait]
pub trait LocalStorageFactory: Send + Sync + 'static {
    /// Storage type that implements the required OpenRaft traits
    type Storage: openraft::storage::RaftLogStorage<LocalTypeConfig>
        + openraft::storage::RaftStateMachine<LocalTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Create a new local storage instance for a consensus group
    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage>;
}

/// Factory for memory-based local storage
pub struct MemoryLocalStorageFactory {
    storage_engine: Arc<MemoryStorage>,
}

impl MemoryLocalStorageFactory {
    /// Create a new memory storage factory
    pub fn new() -> Self {
        Self {
            storage_engine: Arc::new(MemoryStorage::new()),
        }
    }
}

impl Default for MemoryLocalStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LocalStorageFactory for MemoryLocalStorageFactory {
    type Storage = LocalStorage<MemoryStorage>;

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        LocalStorage::new(self.storage_engine.clone(), group_id, local_state).await
    }
}

/// Factory for RocksDB-based local storage
pub struct RocksDBLocalStorageFactory {
    storage_engine: Arc<RocksDBStorage>,
}

impl RocksDBLocalStorageFactory {
    /// Create a new RocksDB storage factory
    pub async fn new(base_path: std::path::PathBuf) -> ConsensusResult<Self> {
        let config = crate::storage::adaptors::rocksdb::RocksDBAdaptorConfig {
            path: base_path,
            ..Default::default()
        };
        let storage_engine = Arc::new(
            RocksDBStorage::new(config)
                .await
                .map_err(|e| Error::storage(format!("Failed to create RocksDB storage: {}", e)))?,
        );
        Ok(Self { storage_engine })
    }
}

#[async_trait::async_trait]
impl LocalStorageFactory for RocksDBLocalStorageFactory {
    type Storage = LocalStorage<RocksDBStorage>;

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        LocalStorage::new(self.storage_engine.clone(), group_id, local_state).await
    }
}

/// Unified local storage type that can be either memory or RocksDB
pub enum UnifiedLocalStorage {
    /// Memory-backed local storage
    Memory(LocalStorage<MemoryStorage>),
    /// RocksDB-backed local storage
    RocksDB(LocalStorage<RocksDBStorage>),
}

// Implement Clone for UnifiedLocalStorage
impl Clone for UnifiedLocalStorage {
    fn clone(&self) -> Self {
        match self {
            UnifiedLocalStorage::Memory(storage) => UnifiedLocalStorage::Memory(storage.clone()),
            UnifiedLocalStorage::RocksDB(storage) => UnifiedLocalStorage::RocksDB(storage.clone()),
        }
    }
}

// Delegate all OpenRaft traits to the inner storage
impl openraft::storage::RaftLogReader<LocalTypeConfig> for UnifiedLocalStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<LocalTypeConfig>>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.try_get_log_entries(range).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<LocalTypeConfig>>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.read_vote().await,
            UnifiedLocalStorage::RocksDB(storage) => storage.read_vote().await,
        }
    }
}

impl openraft::storage::RaftLogStorage<LocalTypeConfig> for UnifiedLocalStorage {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.save_vote(vote).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.save_vote(vote).await,
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::IOFlushed<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<LocalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.append(entries, callback).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.truncate(log_id).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.purge(log_id).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<LocalTypeConfig>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.get_log_state().await,
            UnifiedLocalStorage::RocksDB(storage) => storage.get_log_state().await,
        }
    }
}

/// Unified snapshot builder
pub enum UnifiedLocalSnapshotBuilder {
    /// Memory-based snapshot builder
    Memory(super::unified::LocalSnapshotBuilder<MemoryStorage>),
    /// RocksDB-based snapshot builder
    RocksDB(super::unified::LocalSnapshotBuilder<RocksDBStorage>),
}

impl openraft::RaftSnapshotBuilder<LocalTypeConfig> for UnifiedLocalSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<LocalTypeConfig>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            UnifiedLocalSnapshotBuilder::Memory(builder) => builder.build_snapshot().await,
            UnifiedLocalSnapshotBuilder::RocksDB(builder) => builder.build_snapshot().await,
        }
    }
}

impl openraft::storage::RaftStateMachine<LocalTypeConfig> for UnifiedLocalStorage {
    type SnapshotBuilder = UnifiedLocalSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<LocalTypeConfig>>,
            openraft::StoredMembership<LocalTypeConfig>,
        ),
        openraft::StorageError<LocalTypeConfig>,
    > {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.applied_state().await,
            UnifiedLocalStorage::RocksDB(storage) => storage.applied_state().await,
        }
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<crate::local::LocalResponse>, openraft::StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = openraft::Entry<LocalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.apply(entries).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.apply(entries).await,
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        match self {
            UnifiedLocalStorage::Memory(storage) => {
                UnifiedLocalSnapshotBuilder::Memory(storage.get_snapshot_builder().await)
            }
            UnifiedLocalStorage::RocksDB(storage) => {
                UnifiedLocalSnapshotBuilder::RocksDB(storage.get_snapshot_builder().await)
            }
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, openraft::StorageError<LocalTypeConfig>> {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.begin_receiving_snapshot().await,
            UnifiedLocalStorage::RocksDB(storage) => storage.begin_receiving_snapshot().await,
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<LocalTypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.install_snapshot(meta, snapshot).await,
            UnifiedLocalStorage::RocksDB(storage) => storage.install_snapshot(meta, snapshot).await,
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<LocalTypeConfig>>,
        openraft::StorageError<LocalTypeConfig>,
    > {
        match self {
            UnifiedLocalStorage::Memory(storage) => storage.get_current_snapshot().await,
            UnifiedLocalStorage::RocksDB(storage) => storage.get_current_snapshot().await,
        }
    }
}

/// Factory that creates unified local storage based on configuration
pub struct UnifiedLocalStorageFactory {
    config: StorageConfig,
}

impl UnifiedLocalStorageFactory {
    /// Create a new unified factory
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl LocalStorageFactory for UnifiedLocalStorageFactory {
    type Storage = UnifiedLocalStorage;

    async fn create_local_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self::Storage> {
        match &self.config {
            StorageConfig::Memory => {
                let factory = MemoryLocalStorageFactory::new();
                let storage = factory.create_local_storage(group_id, local_state).await?;
                Ok(UnifiedLocalStorage::Memory(storage))
            }
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBLocalStorageFactory::new(path.join("raft")).await?;
                let storage = factory.create_local_storage(group_id, local_state).await?;
                Ok(UnifiedLocalStorage::RocksDB(storage))
            }
        }
    }
}

/// Create a local storage factory based on configuration
pub fn create_local_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<Box<dyn LocalStorageFactory<Storage = UnifiedLocalStorage>>> {
    Ok(Box::new(UnifiedLocalStorageFactory::new(config.clone())))
}
