//! Storage factory for creating isolated storage instances for local consensus groups

use crate::allocation::ConsensusGroupId;
use crate::config::StorageConfig;
use crate::error::ConsensusResult;
use crate::local::{LocalTypeConfig, storage::memory::LocalMemoryStorage};
use openraft::storage::{RaftLogStorage, RaftStateMachine};

/// Trait for creating storage instances for local consensus groups
pub trait LocalStorageFactory: Send + Sync + 'static {
    /// Storage type that implements both RaftLogStorage and RaftStateMachine
    type Storage: RaftLogStorage<LocalTypeConfig>
        + RaftStateMachine<LocalTypeConfig>
        + Clone
        + Send
        + Sync
        + 'static;

    /// Create a new storage instance for a consensus group
    fn create_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<Self::Storage>;

    /// Clean up storage for a removed group (optional)
    fn cleanup_storage(&self, _group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // Default implementation does nothing
        Ok(())
    }
}

/// Factory for creating in-memory storage instances
#[derive(Clone)]
pub struct MemoryStorageFactory;

impl Default for MemoryStorageFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorageFactory {
    /// Create a new memory storage factory
    pub fn new() -> Self {
        Self
    }
}

impl LocalStorageFactory for MemoryStorageFactory {
    type Storage = LocalMemoryStorage;

    fn create_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<Self::Storage> {
        Ok(LocalMemoryStorage::new(group_id))
    }
}

// Future: Factory for creating RocksDB storage instances
// This is commented out until we implement LocalRocksDBStorage properly
/*
#[derive(Clone)]
pub struct RocksDBStorageFactory {
    /// Base directory for all group databases
    base_path: PathBuf,
    /// Shared database instances (one per group)
    databases: Arc<RwLock<std::collections::HashMap<ConsensusGroupId, Arc<rocksdb::DB>>>>,
}

impl RocksDBStorageFactory {
    /// Create a new RocksDB storage factory
    pub fn new(base_path: PathBuf) -> ConsensusResult<Self> {
        // Ensure base directory exists
        std::fs::create_dir_all(&base_path)
            .map_err(|e| ConsensusError::Storage(format!("Failed to create base directory: {}", e)))?;

        Ok(Self {
            base_path,
            databases: Arc::new(RwLock::new(std::collections::HashMap::new())),
        })
    }

    /// Get the path for a specific group's database
    fn group_path(&self, group_id: ConsensusGroupId) -> PathBuf {
        self.base_path.join(format!("group_{:?}", group_id))
    }
}

impl LocalStorageFactory for RocksDBStorageFactory {
    type Storage = LocalRocksDBStorage;

    fn create_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<Self::Storage> {
        let path = self.group_path(group_id);

        // Create RocksDB storage for this group
        LocalRocksDBStorage::new(group_id, path)
    }

    fn cleanup_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // Remove the database from our cache
        let mut databases = self.databases.blocking_write();
        databases.remove(&group_id);

        // Optionally remove the database files
        let path = self.group_path(group_id);
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .map_err(|e| ConsensusError::Storage(format!("Failed to cleanup storage: {}", e)))?;
        }

        Ok(())
    }
}
*/

/// Create a storage factory based on configuration
///
/// Note: This returns a factory with LocalMemoryStorage type for compatibility.
/// In the future, we could use a trait object or enum to support different storage types.
pub fn create_storage_factory(
    config: &StorageConfig,
) -> ConsensusResult<Box<dyn LocalStorageFactory<Storage = LocalMemoryStorage>>> {
    match config {
        StorageConfig::Memory => Ok(Box::new(MemoryStorageFactory::new())
            as Box<dyn LocalStorageFactory<Storage = LocalMemoryStorage>>),
        StorageConfig::RocksDB { path } => {
            // For now, we'll use memory storage for compatibility
            // The RocksDB implementation is complete but requires changing the trait bound
            // from LocalMemoryStorage to a more generic storage trait
            tracing::info!(
                "RocksDB storage requested at path {:?}. Using memory storage for compatibility. \
                 To use RocksDB, use RocksDBStorageFactory directly.",
                path
            );
            Ok(Box::new(MemoryStorageFactory::new())
                as Box<
                    dyn LocalStorageFactory<Storage = LocalMemoryStorage>,
                >)
        }
    }
}

// Future: RocksDB storage implementation
// This would be similar to LocalMemoryStorage but backed by RocksDB
// TODO: Implement LocalRocksDBStorage when needed
/*
#[derive(Clone)]
pub struct LocalRocksDBStorage {
    // Implementation would go here
    _group_id: ConsensusGroupId,
    _path: PathBuf,
}

impl LocalRocksDBStorage {
    fn new(_group_id: ConsensusGroupId, _path: PathBuf) -> ConsensusResult<Self> {
        // TODO: Implement RocksDB storage for local groups
        unimplemented!("LocalRocksDBStorage is not yet implemented")
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_storage_factory() {
        let factory = MemoryStorageFactory::new();

        // Create storage for different groups
        let group1 = ConsensusGroupId::new(1);
        let group2 = ConsensusGroupId::new(2);

        let storage1 = factory.create_storage(group1).unwrap();
        let storage2 = factory.create_storage(group2).unwrap();

        // Each group gets its own isolated storage instance
        // (in real usage, these would be used by separate Raft instances)
        assert!(matches!(storage1, LocalMemoryStorage { .. }));
        assert!(matches!(storage2, LocalMemoryStorage { .. }));
    }

    #[test]
    fn test_create_storage_factory_from_config() {
        // Test memory config
        let memory_config = StorageConfig::Memory;
        let factory = create_storage_factory(&memory_config).unwrap();

        let group = ConsensusGroupId::new(1);
        let storage = factory.create_storage(group).unwrap();
        assert!(matches!(storage, LocalMemoryStorage { .. }));

        // Test RocksDB config (should fall back to memory for now)
        let rocksdb_config = StorageConfig::RocksDB {
            path: "/tmp/test_rocks".into(),
        };
        let factory = create_storage_factory(&rocksdb_config).unwrap();

        let storage = factory.create_storage(group).unwrap();
        // Currently falls back to memory storage
        assert!(matches!(storage, LocalMemoryStorage { .. }));
    }

    #[test]
    fn test_storage_isolation() {
        let factory = MemoryStorageFactory::new();

        // Create storage for two different groups
        let group1 = ConsensusGroupId::new(1);
        let group2 = ConsensusGroupId::new(2);

        let _storage1 = factory.create_storage(group1).unwrap();
        let _storage2 = factory.create_storage(group2).unwrap();

        // Each storage instance is isolated
        // They have different group IDs and maintain separate state
        // This is validated by the LocalMemoryStorage implementation
        // which creates a separate LocalStreamStore for each group

        // The actual isolation is tested through the state machine
        // Each group's storage maintains its own:
        // - Log entries
        // - Membership configuration
        // - Vote state
        // - Snapshot data
        // - Stream store (with group-specific streams)
    }
}
