//! RocksDB storage factory implementation

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::config::RocksDBConfig;
use super::storage::LocalRocksDBStorage;
use crate::allocation::ConsensusGroupId;
use crate::error::ConsensusResult;
use crate::local::storage::factory::LocalStorageFactory;

/// Factory for creating RocksDB storage instances
#[derive(Clone)]
pub struct RocksDBStorageFactory {
    /// Base configuration for all instances
    config: RocksDBConfig,
    /// Cache of created storage instances
    instances: Arc<Mutex<HashMap<ConsensusGroupId, LocalRocksDBStorage>>>,
}

impl RocksDBStorageFactory {
    /// Create a new RocksDB storage factory
    pub fn new(config: RocksDBConfig) -> ConsensusResult<Self> {
        // Ensure base directory exists
        std::fs::create_dir_all(&config.base_path).map_err(|e| {
            crate::error::Error::Storage(format!("Failed to create base directory: {}", e))
        })?;

        Ok(Self {
            config,
            instances: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Create factory with development configuration
    pub fn development(base_path: PathBuf) -> ConsensusResult<Self> {
        let mut config = RocksDBConfig::development();
        config.base_path = base_path;
        Self::new(config)
    }

    /// Create factory with production configuration
    pub fn production(base_path: PathBuf) -> ConsensusResult<Self> {
        let mut config = RocksDBConfig::production();
        config.base_path = base_path;
        Self::new(config)
    }

    /// Get the path for a specific group's database
    fn group_path(&self, group_id: ConsensusGroupId) -> PathBuf {
        self.config.base_path.join(format!("group_{:?}", group_id))
    }
}

impl LocalStorageFactory for RocksDBStorageFactory {
    type Storage = LocalRocksDBStorage;

    fn create_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<Self::Storage> {
        // Check cache first
        // Check cache first
        {
            let instances = self.instances.lock().unwrap();
            if let Some(storage) = instances.get(&group_id) {
                return Ok(storage.clone());
            }
        }

        // Create new storage instance
        let storage = LocalRocksDBStorage::new(group_id, self.config.clone())?;

        // Cache the instance
        // Cache the instance
        {
            let mut instances = self.instances.lock().unwrap();
            instances.insert(group_id, storage.clone());
        }

        Ok(storage)
    }

    fn cleanup_storage(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // Remove from cache
        // Remove from cache
        {
            let mut instances = self.instances.lock().unwrap();
            instances.remove(&group_id);
        }

        // Optionally remove the database files
        let path = self.group_path(group_id);
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(|e| {
                crate::error::Error::Storage(format!("Failed to cleanup storage: {}", e))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_rocksdb_factory_creation() {
        let temp_dir = tempdir().unwrap();
        let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

        // Create storage for different groups
        let group1 = ConsensusGroupId::new(1);
        let group2 = ConsensusGroupId::new(2);

        let storage1 = factory.create_storage(group1).unwrap();
        let storage2 = factory.create_storage(group2).unwrap();

        // Verify they're different instances
        assert_ne!(
            format!("{:?}", storage1.group_id),
            format!("{:?}", storage2.group_id)
        );

        // Verify caching works
        let storage1_again = factory.create_storage(group1).unwrap();
        // Both should have the same group_id
        assert_eq!(
            format!("{:?}", storage1.group_id),
            format!("{:?}", storage1_again.group_id)
        );
    }

    #[test]
    fn test_cleanup_storage() {
        let temp_dir = tempdir().unwrap();
        let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

        let group_id = ConsensusGroupId::new(1);
        let _storage = factory.create_storage(group_id).unwrap();

        // Verify directory exists
        let group_path = factory.group_path(group_id);
        assert!(group_path.exists());

        // Cleanup
        factory.cleanup_storage(group_id).unwrap();

        // Verify directory is removed
        assert!(!group_path.exists());
    }

    #[test]
    fn test_rocksdb_factory_cleanup_complete() {
        let temp_dir = tempdir().unwrap();
        let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

        // Create storage
        let group_id = ConsensusGroupId::new(1);
        let storage = factory.create_storage(group_id).unwrap();

        // Verify directory exists
        assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());

        // Drop storage to release lock
        drop(storage);

        // Cleanup
        factory.cleanup_storage(group_id).unwrap();

        // Verify directory is gone
        assert!(!temp_dir.path().join("group_ConsensusGroupId(1)").exists());

        // Should be able to create again
        let storage_new = factory.create_storage(group_id).unwrap();
        assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
        drop(storage_new);
    }

    #[test]
    fn test_rocksdb_factory_isolation() {
        let temp_dir = tempdir().unwrap();
        let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

        // Create multiple groups
        let groups = vec![
            ConsensusGroupId::new(1),
            ConsensusGroupId::new(2),
            ConsensusGroupId::new(3),
        ];

        let storages: Vec<_> = groups
            .iter()
            .map(|&id| factory.create_storage(id).unwrap())
            .collect();

        // Verify all directories exist
        for id in &groups {
            assert!(temp_dir.path().join(format!("group_{:?}", id)).exists());
        }

        // Drop middle storage and cleanup
        drop(storages);
        factory.cleanup_storage(groups[1]).unwrap();

        // Verify only middle one is gone
        assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
        assert!(!temp_dir.path().join("group_ConsensusGroupId(2)").exists());
        assert!(temp_dir.path().join("group_ConsensusGroupId(3)").exists());
    }

    #[test]
    fn test_rocksdb_factory_error_handling() {
        // Test with invalid path
        let result = RocksDBStorageFactory::development("/invalid/path/that/does/not/exist".into());
        assert!(result.is_err());

        // Test cleanup of non-existent group
        let temp_dir = tempdir().unwrap();
        let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

        // This should not panic, just return Ok
        let result = factory.cleanup_storage(ConsensusGroupId::new(999));
        assert!(result.is_ok());
    }
}
