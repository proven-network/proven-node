//! Basic tests for RocksDB storage

use proven_consensus::{
    allocation::ConsensusGroupId,
    local::storage::rocksdb::{LocalRocksDBStorage, RocksDBConfig},
};
use tempfile::tempdir;

#[test]
fn test_rocksdb_creation() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Create storage
    let _storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config.clone()).unwrap();

    // Verify it was created
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());

    // Create another group
    let _storage2 = LocalRocksDBStorage::new(ConsensusGroupId::new(2), config).unwrap();

    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
}

#[test]
fn test_rocksdb_factory() {
    use proven_consensus::local::{
        storage::factory::LocalStorageFactory, storage::rocksdb::RocksDBStorageFactory,
    };

    let temp_dir = tempdir().unwrap();
    let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

    // Create storage instances
    let group1 = ConsensusGroupId::new(1);
    let group2 = ConsensusGroupId::new(2);

    let _storage1 = factory.create_storage(group1).unwrap();
    let _storage2 = factory.create_storage(group2).unwrap();

    // Verify directories were created
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());

    // Test cleanup
    factory.cleanup_storage(group1).unwrap();
    assert!(!temp_dir.path().join("group_ConsensusGroupId(1)").exists());
}
