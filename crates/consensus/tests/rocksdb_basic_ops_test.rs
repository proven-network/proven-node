//! Basic operation tests for RocksDB storage without Raft specifics

use bytes::Bytes;
use proven_consensus::{
    allocation::ConsensusGroupId,
    local::{
        storage::factory::LocalStorageFactory,
        storage::{LocalRocksDBStorage, RocksDBConfig},
    },
};
use tempfile::tempdir;

#[tokio::test]
async fn test_rocksdb_creation_and_factory() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Test direct creation
    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config.clone()).unwrap();
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    drop(storage);

    // Test factory creation
    use proven_consensus::local::storage::RocksDBStorageFactory;
    let factory = RocksDBStorageFactory::development(temp_dir.path().to_path_buf()).unwrap();

    let group1 = ConsensusGroupId::new(1);
    let group2 = ConsensusGroupId::new(2);

    let _storage1 = factory.create_storage(group1).unwrap();
    let _storage2 = factory.create_storage(group2).unwrap();

    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());

    // Test cleanup
    factory.cleanup_storage(group1).unwrap();
    assert!(!temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
}

#[tokio::test]
async fn test_rocksdb_state_machine_operations() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Test stream operations through state machine
    let mut state_machine = storage.state_machine.write().await;

    // Create a stream
    state_machine.add_stream(
        "test-stream".to_string(),
        proven_consensus::local::state_machine::StreamData {
            messages: std::collections::BTreeMap::new(),
            last_seq: 0,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        },
    );

    // Publish a message
    let result = state_machine.publish_message("test-stream", Bytes::from("test-data"), None);

    assert!(result.is_ok());
    let seq = result.unwrap();
    assert_eq!(seq, 1);

    // Get metrics
    let metrics = state_machine.get_metrics();
    assert_eq!(metrics.stream_count, 1);
    assert_eq!(metrics.total_messages, 1);

    // Test pause/resume
    let pause_result = state_machine.pause_stream("test-stream");
    assert!(pause_result.is_ok());
    assert!(state_machine.is_stream_paused("test-stream"));

    let resume_result = state_machine.resume_stream("test-stream");
    assert!(resume_result.is_ok());
    assert!(!state_machine.is_stream_paused("test-stream"));
}

// TODO: This test requires access to internal methods for creating stream metadata
// #[tokio::test]
#[allow(dead_code)]
async fn test_rocksdb_stream_storage_operations() {
    use proven_consensus::local::storage::traits::StreamStorage;

    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    let storage = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config).unwrap();

    // Create stream metadata in RocksDB
    {
        use proven_consensus::local::storage::traits::{
            CompressionType, StreamConfig, StreamMetadata,
        };
        use std::collections::HashMap;

        let _metadata = StreamMetadata {
            name: "export-test".to_string(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            modified_at: chrono::Utc::now().timestamp_millis() as u64,
            last_sequence: 0,
            message_count: 0,
            config: StreamConfig {
                retention_seconds: None,
                max_messages: None,
                compact_on_deletion: false,
                compression: CompressionType::None,
            },
            is_paused: false,
            custom_metadata: HashMap::new(),
        };

        // This is a hack - we need to make update_stream_metadata public or add a proper API
        // For now, let's skip this test
    }

    // Test stream exists
    let exists = storage.stream_exists("export-test").await;
    assert!(exists.is_ok());
    assert!(exists.unwrap());

    // Test export
    let export_result = storage.export_stream("export-test").await;
    assert!(export_result.is_ok());
    let export = export_result.unwrap();
    assert_eq!(export.metadata.name, "export-test");

    // Test metrics
    let metrics = storage.get_stream_metrics("export-test").await;
    assert!(metrics.is_ok());
    let metrics = metrics.unwrap();
    assert_eq!(metrics.stream_name, "export-test");

    // Test checkpoint creation
    let checkpoint = storage.create_checkpoint("export-test", None).await;
    assert!(checkpoint.is_ok());
    let checkpoint = checkpoint.unwrap();
    assert_eq!(checkpoint.stream_name, "export-test");
    assert_eq!(checkpoint.data.messages.len(), 5);
}

#[tokio::test]
async fn test_rocksdb_isolation_between_groups() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Create two separate storage instances for different groups
    let storage1 = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config.clone()).unwrap();

    let storage2 = LocalRocksDBStorage::new(ConsensusGroupId::new(2), config).unwrap();

    // Add streams to each group
    {
        let mut sm1 = storage1.state_machine.write().await;
        sm1.add_stream(
            "group1-stream".to_string(),
            proven_consensus::local::state_machine::StreamData {
                messages: std::collections::BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );
        sm1.publish_message("group1-stream", Bytes::from("data1"), None)
            .unwrap();
    }

    {
        let mut sm2 = storage2.state_machine.write().await;
        sm2.add_stream(
            "group2-stream".to_string(),
            proven_consensus::local::state_machine::StreamData {
                messages: std::collections::BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );
        sm2.publish_message("group2-stream", Bytes::from("data2"), None)
            .unwrap();
    }

    // Verify isolation
    {
        let sm1 = storage1.state_machine.read().await;
        let sm2 = storage2.state_machine.read().await;

        // Group 1 should only have its stream
        assert!(sm1.get_stream("group1-stream").is_some());
        assert!(sm1.get_stream("group2-stream").is_none());

        // Group 2 should only have its stream
        assert!(sm2.get_stream("group1-stream").is_none());
        assert!(sm2.get_stream("group2-stream").is_some());
    }

    // Verify separate directories
    assert!(temp_dir.path().join("group_ConsensusGroupId(1)").exists());
    assert!(temp_dir.path().join("group_ConsensusGroupId(2)").exists());
}
