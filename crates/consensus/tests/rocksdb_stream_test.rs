//! Test RocksDB storage implementation through the StreamStorage trait

use proven_consensus::{
    allocation::ConsensusGroupId,
    local::storage::{LocalRocksDBStorage, RocksDBConfig, traits::StreamStorage},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_rocksdb_stream_operations() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Test stream doesn't exist initially
    let exists = storage.stream_exists("test-stream").await.unwrap();
    assert!(!exists);

    // Test export on non-existent stream should fail
    let export_result = storage.export_stream("test-stream").await;
    assert!(export_result.is_err());

    // Test metrics on non-existent stream should fail
    let metrics_result = storage.get_stream_metrics("test-stream").await;
    assert!(metrics_result.is_err());
}

#[test]
fn test_rocksdb_factory_and_cleanup() {
    use proven_consensus::local::{
        storage::RocksDBStorageFactory, storage::factory::LocalStorageFactory,
    };

    let temp_dir = tempdir().unwrap();
    let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

    // Create storage for multiple groups
    let group1 = ConsensusGroupId::new(1);
    let group2 = ConsensusGroupId::new(2);
    let group3 = ConsensusGroupId::new(3);

    let _storage1 = factory.create_storage(group1).unwrap();
    let _storage2 = factory.create_storage(group2).unwrap();
    let _storage3 = factory.create_storage(group3).unwrap();

    // Verify directories exist
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(3)").exists());

    // Drop storage2 before cleanup to release the RocksDB lock
    drop(_storage2);

    // Cleanup one group
    factory.cleanup_storage(group2).unwrap();

    // Verify only group2 was cleaned up
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(!temp_dir.path().join("group_ConsensusGroupId(2)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(3)").exists());

    // Creating storage for the same group again should work
    let _storage2_new = factory.create_storage(group2).unwrap();
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
}

#[test]
fn test_rocksdb_configuration() {
    use proven_consensus::local::storage::traits::WalConfig;

    let temp_dir = tempdir().unwrap();

    // Test with custom configuration
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        max_open_files: 1000,
        max_background_jobs: 8,
        enable_statistics: true,
        max_total_wal_size: 2 * 1024 * 1024 * 1024, // 2GB
        wal_config: WalConfig {
            sync_on_commit: false,
            max_wal_size: 2 * 1024 * 1024 * 1024, // 2GB
            wal_ttl_seconds: 7200,
            wal_size_limit_mb: 2048,
        },
        ..RocksDBConfig::default()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Storage should be created successfully
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    drop(storage);

    // Test production configuration
    let mut prod_config = RocksDBConfig::production();
    prod_config.base_path = temp_dir.path().to_path_buf();

    let prod_storage = LocalRocksDBStorage::new(ConsensusGroupId::new(2), prod_config).unwrap();

    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
    drop(prod_storage);
}

#[test]
fn test_rocksdb_multiple_instances() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Create multiple storage instances for different groups
    let storage1 = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config.clone());

    let storage2 = LocalRocksDBStorage::new(ConsensusGroupId::new(2), config.clone());

    let storage3 = LocalRocksDBStorage::new(ConsensusGroupId::new(3), config);

    // All should succeed
    assert!(storage1.is_ok());
    assert!(storage2.is_ok());
    assert!(storage3.is_ok());

    // Verify separate directories
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(3)").exists());
}

#[test]
fn test_rocksdb_error_handling() {
    // Test with invalid path
    let config = RocksDBConfig {
        base_path: std::path::PathBuf::from("/invalid/path/that/does/not/exist"),
        ..RocksDBConfig::development()
    };

    let result = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config);

    // Should fail due to invalid path
    assert!(result.is_err());
}
