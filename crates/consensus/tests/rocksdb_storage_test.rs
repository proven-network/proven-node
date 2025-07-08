//! Simple tests for RocksDB storage implementation focusing on core functionality

use proven_consensus::{
    allocation::ConsensusGroupId,
    local::storage::rocksdb::{LocalRocksDBStorage, RocksDBConfig},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_rocksdb_basic_operations() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Test that storage was created successfully
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());

    // Test column family creation
    let _cf = storage
        .get_or_create_stream_cf("test-stream")
        .await
        .unwrap();
    // Column family was created successfully if we got here

    // Test key encoding/decoding
    let encoded = LocalRocksDBStorage::encode_sequence(12345);
    let decoded = LocalRocksDBStorage::decode_sequence(&encoded).unwrap();
    assert_eq!(decoded, 12345);
}

#[tokio::test]
async fn test_rocksdb_stream_metadata() {
    use proven_consensus::local::storage::traits::{CompressionType, StreamConfig, StreamMetadata};
    use std::collections::HashMap;

    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Create and update metadata
    let metadata = StreamMetadata {
        name: "test-stream".to_string(),
        created_at: chrono::Utc::now().timestamp_millis() as u64,
        modified_at: chrono::Utc::now().timestamp_millis() as u64,
        last_sequence: 100,
        message_count: 50,
        config: StreamConfig {
            retention_seconds: Some(3600),
            max_messages: Some(1000),
            compact_on_deletion: true,
            compression: CompressionType::Lz4,
        },
        is_paused: false,
        custom_metadata: HashMap::new(),
    };

    // Create the stream CF first
    storage
        .get_or_create_stream_cf("test-stream")
        .await
        .unwrap();

    // Update metadata
    storage.update_stream_metadata(&metadata).await.unwrap();

    // Verify it was saved (we would need a get_metadata method to fully test this)
    // For now, just verify no errors occurred
}

#[tokio::test]
async fn test_rocksdb_key_value_operations() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Test basic put/get operations
    let cf = storage
        .get_or_create_stream_cf("test-stream")
        .await
        .unwrap();

    // Put some data
    let key = b"test-key";
    let value = b"test-value";
    storage.db.put_cf(&cf, key, value).unwrap();

    // Get it back
    let retrieved = storage.db.get_cf(&cf, key).unwrap();
    assert_eq!(retrieved, Some(value.to_vec()));

    // Test non-existent key
    let missing = storage.db.get_cf(&cf, b"missing-key").unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_rocksdb_multiple_groups() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Create storage for multiple groups
    let storage1 = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config.clone()).unwrap();
    let storage2 = LocalRocksDBStorage::new(ConsensusGroupId::new(2), config.clone()).unwrap();
    let storage3 = LocalRocksDBStorage::new(ConsensusGroupId::new(3), config).unwrap();

    // Verify separate directories
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(3)").exists());

    // Test isolation - data in one group shouldn't affect others
    let cf1 = storage1.get_or_create_stream_cf("stream1").await.unwrap();
    storage1.db.put_cf(&cf1, b"key1", b"value1").unwrap();

    // Storage2 shouldn't have this data
    let cf2_result = storage2.db.cf_handle("stream_stream1");
    assert!(cf2_result.is_none());

    drop(storage1);
    drop(storage2);
    drop(storage3);
}

#[tokio::test]
async fn test_rocksdb_persistence() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let group_id = ConsensusGroupId::new(1);

    // Test data persistence within same instance
    let storage = LocalRocksDBStorage::new(group_id, config).unwrap();

    // Write to default CF (which is always opened)
    let default_cf = storage.db.cf_handle("default").unwrap();
    storage
        .db
        .put_cf(default_cf, b"persist-key", b"persist-value")
        .unwrap();

    // Verify data is there
    let value = storage.db.get_cf(default_cf, b"persist-key").unwrap();
    assert_eq!(value, Some(b"persist-value".to_vec()));

    // Note: Testing persistence across database restarts would require
    // implementing column family discovery on startup (Phase 3)
}

#[test]
fn test_rocksdb_configuration() {
    use proven_consensus::local::storage::traits::WalConfig;

    let temp_dir = tempdir().unwrap();

    // Test default configuration
    let default_config = RocksDBConfig::default();
    assert_eq!(default_config.max_open_files, -1); // -1 means unlimited
    assert!(!default_config.enable_statistics);

    // Test development configuration
    let dev_config = RocksDBConfig::development();
    assert!(dev_config.enable_statistics);
    assert!(dev_config.wal_config.sync_on_commit);

    // Test production configuration
    let prod_config = RocksDBConfig::production();
    assert!(!prod_config.enable_statistics);
    assert_eq!(prod_config.max_open_files, -1); // Production also uses unlimited
    assert!(prod_config.wal_config.sync_on_commit);

    // Test custom configuration
    let custom_config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        max_open_files: 1000,
        max_background_jobs: 8,
        enable_statistics: true,
        max_total_wal_size: 2 * 1024 * 1024 * 1024, // 2GB
        wal_config: WalConfig {
            sync_on_commit: false,
            max_wal_size: 2 * 1024 * 1024 * 1024,
            wal_ttl_seconds: 7200,
            wal_size_limit_mb: 2048,
        },
        ..RocksDBConfig::default()
    };

    // Create storage with custom config
    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), custom_config);
    assert!(storage.is_ok());
}
