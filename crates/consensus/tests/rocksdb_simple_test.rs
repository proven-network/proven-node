//! Simple test for RocksDB creation without complex operations

use proven_consensus::{
    allocation::ConsensusGroupId,
    local::storage::{LocalRocksDBStorage, RocksDBConfig},
};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_rocksdb_basic_creation() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Test creation
    let result = LocalRocksDBStorage::new(ConsensusGroupId::new(1), config);

    match result {
        Ok(_storage) => {
            println!("RocksDB storage created successfully");
            // Check directory exists
            let expected_path = temp_dir.path().join("group_ConsensusGroupId(1)");
            assert!(
                expected_path.exists(),
                "Expected directory not found: {:?}",
                expected_path
            );

            // List contents
            if let Ok(entries) = fs::read_dir(&expected_path) {
                println!("Directory contents:");
                for entry in entries.flatten() {
                    println!("  - {:?}", entry.file_name());
                }
            }
        }
        Err(e) => {
            panic!("Failed to create RocksDB storage: {:?}", e);
        }
    }
}

#[test]
fn test_multiple_groups_creation() {
    let temp_dir = tempdir().unwrap();
    let config = RocksDBConfig {
        base_path: temp_dir.path().to_path_buf(),
        ..RocksDBConfig::development()
    };

    // Create multiple groups
    for i in 1..=3 {
        let result = LocalRocksDBStorage::new(ConsensusGroupId::new(i), config.clone());

        match result {
            Ok(_storage) => {
                println!("Created storage for group {}", i);
                let expected_path = temp_dir
                    .path()
                    .join(format!("group_ConsensusGroupId({})", i));
                assert!(
                    expected_path.exists(),
                    "Expected directory not found for group {}",
                    i
                );
            }
            Err(e) => {
                panic!("Failed to create storage for group {}: {:?}", i, e);
            }
        }
    }

    // Verify all directories exist
    for i in 1..=3 {
        let path = temp_dir
            .path()
            .join(format!("group_ConsensusGroupId({})", i));
        assert!(path.exists(), "Directory missing for group {}", i);
    }
}
