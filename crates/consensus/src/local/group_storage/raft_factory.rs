//! Factory for creating Raft storage instances for local consensus groups
//!
//! This factory is specifically for creating storage instances that handle
//! Raft consensus operations within a local group.

use crate::allocation::ConsensusGroupId;
use crate::config::StorageConfig;
use crate::error::ConsensusResult;
use crate::local::LocalTypeConfig;
use std::path::PathBuf;
use std::sync::Arc;

/// Trait for creating Raft storage instances for local consensus groups
#[async_trait::async_trait]
pub trait GroupRaftStorageFactory: Send + Sync + 'static {
    /// Storage type that implements the required OpenRaft traits
    type Storage: openraft::storage::RaftLogStorage<LocalTypeConfig>
        + openraft::storage::RaftStateMachine<LocalTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Create a new Raft storage instance for a consensus group
    async fn create_raft_storage(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Self::Storage>;

    /// Clean up Raft storage for a removed group (optional)
    fn cleanup_raft_storage(&self, _group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Factory for creating in-memory Raft storage instances
#[derive(Clone)]
pub struct MemoryRaftStorageFactory;

impl Default for MemoryRaftStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryRaftStorageFactory {
    /// Create a new memory Raft storage factory
    pub fn new() -> Self {
        Self
    }
}

/// Factory for creating RocksDB Raft storage instances
#[derive(Clone)]
pub struct RocksDBRaftStorageFactory {
    /// Base directory for all group databases
    base_path: PathBuf,
}

impl RocksDBRaftStorageFactory {
    /// Create a new RocksDB Raft storage factory
    pub fn new(base_path: PathBuf) -> ConsensusResult<Self> {
        // Ensure base directory exists
        std::fs::create_dir_all(&base_path).map_err(|e| {
            crate::error::Error::Storage(format!("Failed to create base directory: {}", e))
        })?;

        Ok(Self { base_path })
    }

    /// Get the path for a specific group's Raft database
    fn group_raft_path(&self, group_id: ConsensusGroupId) -> PathBuf {
        self.base_path.join(format!("group_{:?}_raft", group_id))
    }
}

#[async_trait::async_trait]
impl GroupRaftStorageFactory for MemoryRaftStorageFactory {
    type Storage =
        crate::local::group_storage::RaftStorage<crate::storage::adaptors::memory::MemoryStorage>;

    async fn create_raft_storage(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Self::Storage> {
        // Create memory storage using the storage adaptor for Raft
        let storage_engine = Arc::new(crate::storage::adaptors::memory::MemoryStorage::new());
        let raft_storage =
            crate::local::group_storage::RaftStorage::new(storage_engine, group_id).await?;
        Ok(raft_storage)
    }
}

#[async_trait::async_trait]
impl GroupRaftStorageFactory for RocksDBRaftStorageFactory {
    type Storage =
        crate::local::group_storage::RaftStorage<crate::storage::adaptors::rocksdb::RocksDBStorage>;

    async fn create_raft_storage(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Self::Storage> {
        // Create RocksDB storage for Raft
        let group_path = self.group_raft_path(group_id);
        let config =
            crate::storage::adaptors::rocksdb::RocksDBAdaptorConfig::development(group_path);
        let storage_engine = Arc::new(
            crate::storage::adaptors::rocksdb::RocksDBStorage::new(config)
                .await
                .map_err(|e| crate::error::Error::Storage(e.to_string()))?,
        );
        let raft_storage =
            crate::local::group_storage::RaftStorage::new(storage_engine, group_id).await?;
        Ok(raft_storage)
    }

    fn cleanup_raft_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        let group_path = self.group_raft_path(group_id);
        if group_path.exists() {
            std::fs::remove_dir_all(&group_path).map_err(|e| {
                crate::error::Error::Storage(format!("Failed to cleanup Raft storage: {}", e))
            })?;
        }
        Ok(())
    }
}

/// Unified Raft storage type that can be either memory or RocksDB
pub enum GroupRaftStorage {
    /// Memory-backed Raft storage
    Memory(
        crate::local::group_storage::RaftStorage<crate::storage::adaptors::memory::MemoryStorage>,
    ),
    /// RocksDB-backed Raft storage
    RocksDB(
        crate::local::group_storage::RaftStorage<crate::storage::adaptors::rocksdb::RocksDBStorage>,
    ),
}

// Implement Clone for UnifiedRaftStorage
impl Clone for GroupRaftStorage {
    fn clone(&self) -> Self {
        match self {
            GroupRaftStorage::Memory(storage) => GroupRaftStorage::Memory(storage.clone()),
            GroupRaftStorage::RocksDB(storage) => GroupRaftStorage::RocksDB(storage.clone()),
        }
    }
}

// Delegate all OpenRaft traits to the inner storage
impl openraft::storage::RaftLogReader<LocalTypeConfig> for GroupRaftStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<LocalTypeConfig>>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            GroupRaftStorage::Memory(storage) => storage.try_get_log_entries(range).await,
            GroupRaftStorage::RocksDB(storage) => storage.try_get_log_entries(range).await,
        }
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<LocalTypeConfig>>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            GroupRaftStorage::Memory(storage) => storage.read_vote().await,
            GroupRaftStorage::RocksDB(storage) => storage.read_vote().await,
        }
    }
}

impl openraft::storage::RaftLogStorage<LocalTypeConfig> for GroupRaftStorage {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            GroupRaftStorage::Memory(storage) => storage.save_vote(vote).await,
            GroupRaftStorage::RocksDB(storage) => storage.save_vote(vote).await,
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
            GroupRaftStorage::Memory(storage) => storage.append(entries, callback).await,
            GroupRaftStorage::RocksDB(storage) => storage.append(entries, callback).await,
        }
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            GroupRaftStorage::Memory(storage) => storage.truncate(log_id).await,
            GroupRaftStorage::RocksDB(storage) => storage.truncate(log_id).await,
        }
    }

    async fn purge(
        &mut self,
        log_id: openraft::LogId<LocalTypeConfig>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            GroupRaftStorage::Memory(storage) => storage.purge(log_id).await,
            GroupRaftStorage::RocksDB(storage) => storage.purge(log_id).await,
        }
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<LocalTypeConfig>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            GroupRaftStorage::Memory(storage) => storage.get_log_state().await,
            GroupRaftStorage::RocksDB(storage) => storage.get_log_state().await,
        }
    }
}

/// Unified snapshot builder type
pub enum UnifiedSnapshotBuilder {
    /// Memory-backed snapshot builder
    Memory(
        crate::local::group_storage::RaftSnapshotBuilder<
            crate::storage::adaptors::memory::MemoryStorage,
        >,
    ),
    /// RocksDB-backed snapshot builder
    RocksDB(
        crate::local::group_storage::RaftSnapshotBuilder<
            crate::storage::adaptors::rocksdb::RocksDBStorage,
        >,
    ),
}

impl openraft::RaftSnapshotBuilder<LocalTypeConfig> for UnifiedSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<LocalTypeConfig>, openraft::StorageError<LocalTypeConfig>>
    {
        match self {
            UnifiedSnapshotBuilder::Memory(builder) => builder.build_snapshot().await,
            UnifiedSnapshotBuilder::RocksDB(builder) => builder.build_snapshot().await,
        }
    }
}

impl openraft::storage::RaftStateMachine<LocalTypeConfig> for GroupRaftStorage {
    type SnapshotBuilder = UnifiedSnapshotBuilder;

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
            GroupRaftStorage::Memory(storage) => storage.applied_state().await,
            GroupRaftStorage::RocksDB(storage) => storage.applied_state().await,
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
            GroupRaftStorage::Memory(storage) => storage.apply(entries).await,
            GroupRaftStorage::RocksDB(storage) => storage.apply(entries).await,
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        match self {
            GroupRaftStorage::Memory(storage) => {
                UnifiedSnapshotBuilder::Memory(storage.get_snapshot_builder().await)
            }
            GroupRaftStorage::RocksDB(storage) => {
                UnifiedSnapshotBuilder::RocksDB(storage.get_snapshot_builder().await)
            }
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<std::io::Cursor<Vec<u8>>, openraft::StorageError<LocalTypeConfig>> {
        match self {
            GroupRaftStorage::Memory(storage) => storage.begin_receiving_snapshot().await,
            GroupRaftStorage::RocksDB(storage) => storage.begin_receiving_snapshot().await,
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<LocalTypeConfig>,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), openraft::StorageError<LocalTypeConfig>> {
        match self {
            GroupRaftStorage::Memory(storage) => storage.install_snapshot(meta, snapshot).await,
            GroupRaftStorage::RocksDB(storage) => storage.install_snapshot(meta, snapshot).await,
        }
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<LocalTypeConfig>>,
        openraft::StorageError<LocalTypeConfig>,
    > {
        match self {
            GroupRaftStorage::Memory(storage) => storage.get_current_snapshot().await,
            GroupRaftStorage::RocksDB(storage) => storage.get_current_snapshot().await,
        }
    }
}

/// Factory that creates UnifiedRaftStorage based on configuration
pub struct UnifiedRaftStorageFactory {
    config: StorageConfig,
}

impl UnifiedRaftStorageFactory {
    /// Create a new UnifiedRaftStorageFactory
    pub fn new(config: StorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl GroupRaftStorageFactory for UnifiedRaftStorageFactory {
    type Storage = GroupRaftStorage;

    async fn create_raft_storage(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Self::Storage> {
        match &self.config {
            StorageConfig::Memory => {
                let factory = MemoryRaftStorageFactory::new();
                let storage = factory.create_raft_storage(group_id).await?;
                Ok(GroupRaftStorage::Memory(storage))
            }
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBRaftStorageFactory::new(path.join("raft"))?;
                let storage = factory.create_raft_storage(group_id).await?;
                Ok(GroupRaftStorage::RocksDB(storage))
            }
        }
    }

    fn cleanup_raft_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        match &self.config {
            StorageConfig::Memory => Ok(()), // Memory storage auto-cleans
            StorageConfig::RocksDB { path } => {
                let factory = RocksDBRaftStorageFactory::new(path.join("raft"))?;
                factory.cleanup_raft_storage(group_id)
            }
        }
    }
}

/// Create a Raft storage factory based on configuration
pub fn create_raft_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<Box<dyn GroupRaftStorageFactory<Storage = GroupRaftStorage>>> {
    Ok(Box::new(UnifiedRaftStorageFactory::new(config.clone())))
}
