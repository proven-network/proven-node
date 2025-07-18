use bytes::Bytes;
use proven_storage::{LogStorage, StorageNamespace};
use proven_storage_rocksdb::RocksDbStorage;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
    let namespace = StorageNamespace::new("test_basic");

    // Test initial bounds (should be None)
    assert_eq!(storage.bounds(&namespace).await.unwrap(), None);

    // Append some entries
    let entries = vec![
        (1, Bytes::from("entry 1")),
        (2, Bytes::from("entry 2")),
        (3, Bytes::from("entry 3")),
    ];
    storage.append(&namespace, entries).await.unwrap();

    // Test bounds after append
    assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((1, 3)));

    // Read entries
    let read = storage.read_range(&namespace, 1, 4).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read[0], (1, Bytes::from("entry 1")));
    assert_eq!(read[1], (2, Bytes::from("entry 2")));
    assert_eq!(read[2], (3, Bytes::from("entry 3")));

    // Read partial range
    let partial = storage.read_range(&namespace, 2, 3).await.unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0], (2, Bytes::from("entry 2")));
}

#[tokio::test]
async fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // First instance - write data
    {
        let storage = RocksDbStorage::new(&path).await.unwrap();
        let namespace = StorageNamespace::new("test_persist");

        let entries = vec![
            (10, Bytes::from("persistent 1")),
            (20, Bytes::from("persistent 2")),
            (30, Bytes::from("persistent 3")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((10, 30)));
    }

    // Second instance - read data
    {
        let storage = RocksDbStorage::new(&path).await.unwrap();
        let namespace = StorageNamespace::new("test_persist");

        // Data should persist
        assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((10, 30)));

        let read = storage.read_range(&namespace, 10, 31).await.unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(read[0], (10, Bytes::from("persistent 1")));
        assert_eq!(read[1], (20, Bytes::from("persistent 2")));
        assert_eq!(read[2], (30, Bytes::from("persistent 3")));
    }
}

#[tokio::test]
async fn test_multiple_namespaces() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();

    let ns1 = StorageNamespace::new("namespace1");
    let ns2 = StorageNamespace::new("namespace2");
    let ns3 = StorageNamespace::new("namespace3");

    // Write to different namespaces
    storage
        .append(&ns1, vec![(1, Bytes::from("ns1 data"))])
        .await
        .unwrap();
    storage
        .append(&ns2, vec![(100, Bytes::from("ns2 data"))])
        .await
        .unwrap();
    storage
        .append(&ns3, vec![(1000, Bytes::from("ns3 data"))])
        .await
        .unwrap();

    // Verify isolation between namespaces
    assert_eq!(storage.bounds(&ns1).await.unwrap(), Some((1, 1)));
    assert_eq!(storage.bounds(&ns2).await.unwrap(), Some((100, 100)));
    assert_eq!(storage.bounds(&ns3).await.unwrap(), Some((1000, 1000)));

    let read1 = storage.read_range(&ns1, 0, 2000).await.unwrap();
    assert_eq!(read1.len(), 1);
    assert_eq!(read1[0], (1, Bytes::from("ns1 data")));

    let read2 = storage.read_range(&ns2, 0, 2000).await.unwrap();
    assert_eq!(read2.len(), 1);
    assert_eq!(read2[0], (100, Bytes::from("ns2 data")));
}

#[tokio::test]
async fn test_engine_like_namespaces() {
    // Test with namespace patterns similar to what the engine uses
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    // First run - create various engine-like namespaces
    {
        let storage = RocksDbStorage::new(&path).await.unwrap();

        let namespaces = [
            "stream_group-1___store_keys__",
            "stream_group-1_store_default",
            "stream_group-1___store_keys___698df805-5623-4e45-97b4-9501b583d444",
            "stream_group-1_store_698df805-5623-4e45-97b4-9501b583d444",
            "stream_group-1___store_keys_____management__",
            "stream_group-1_store___management__",
            "group_1_logs",
            "global_state",
            "global_logs",
        ];

        for (i, ns_name) in namespaces.iter().enumerate() {
            let ns = StorageNamespace::new(*ns_name);
            let data = format!("data for {ns_name}");
            storage
                .append(&ns, vec![((i + 1) as u64, Bytes::from(data))])
                .await
                .unwrap();
        }
    }

    // Second run - verify we can reopen with all those column families
    {
        let storage = RocksDbStorage::new(&path).await.unwrap();

        // Verify we can read from a complex namespace
        let ns = StorageNamespace::new("stream_group-1_store_698df805-5623-4e45-97b4-9501b583d444");
        let read = storage.read_range(&ns, 0, 100).await.unwrap();
        assert_eq!(read.len(), 1);
        assert!(read[0].1.starts_with(b"data for"));
    }
}

#[tokio::test]
async fn test_truncate_after() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
    let namespace = StorageNamespace::new("test_truncate");

    // Append entries
    let entries = vec![
        (1, Bytes::from("entry 1")),
        (2, Bytes::from("entry 2")),
        (3, Bytes::from("entry 3")),
        (4, Bytes::from("entry 4")),
        (5, Bytes::from("entry 5")),
    ];
    storage.append(&namespace, entries).await.unwrap();

    // Truncate after index 3
    storage.truncate_after(&namespace, 3).await.unwrap();

    // Verify bounds updated
    assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((1, 3)));

    // Verify entries 4 and 5 are gone
    let read = storage.read_range(&namespace, 1, 6).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read.last().unwrap().0, 3);
}

#[tokio::test]
async fn test_compact_before() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
    let namespace = StorageNamespace::new("test_compact");

    // Append entries
    let entries = vec![
        (1, Bytes::from("entry 1")),
        (2, Bytes::from("entry 2")),
        (3, Bytes::from("entry 3")),
        (4, Bytes::from("entry 4")),
        (5, Bytes::from("entry 5")),
    ];
    storage.append(&namespace, entries).await.unwrap();

    // Compact before index 3 (should remove 1 and 2)
    storage.compact_before(&namespace, 2).await.unwrap();

    // Verify bounds updated
    assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((3, 5)));

    // Verify entries 1 and 2 are gone
    let read = storage.read_range(&namespace, 1, 6).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read.first().unwrap().0, 3);
}

#[tokio::test]
async fn test_concurrent_access() {
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDbStorage::new(temp_dir.path()).await.unwrap());

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks writing to different namespaces concurrently
    for i in 0..10 {
        let storage_clone = storage.clone();
        tasks.spawn(async move {
            let namespace = StorageNamespace::new(format!("concurrent_{i}"));
            let entries: Vec<(u64, Bytes)> = (1..=100)
                .map(|j| (j, Bytes::from(format!("task {i} entry {j}"))))
                .collect();

            storage_clone.append(&namespace, entries).await.unwrap();

            // Verify writes
            let bounds = storage_clone.bounds(&namespace).await.unwrap();
            assert_eq!(bounds, Some((1, 100)));
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Verify all namespaces have correct data
    for i in 0..10 {
        let namespace = StorageNamespace::new(format!("concurrent_{i}"));
        let read = storage.read_range(&namespace, 1, 101).await.unwrap();
        assert_eq!(read.len(), 100);
        assert_eq!(read[0], (1, Bytes::from(format!("task {i} entry 1"))));
        assert_eq!(read[99], (100, Bytes::from(format!("task {i} entry 100"))));
    }
}

#[tokio::test]
async fn test_large_entries() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
    let namespace = StorageNamespace::new("test_large");

    // Create large entries (1MB each)
    let large_data = vec![0u8; 1024 * 1024];
    let entries: Vec<(u64, Bytes)> = (1..=10)
        .map(|i| (i, Bytes::from(large_data.clone())))
        .collect();

    storage.append(&namespace, entries).await.unwrap();

    // Read back and verify
    let read = storage.read_range(&namespace, 1, 11).await.unwrap();
    assert_eq!(read.len(), 10);
    for (_, data) in read {
        assert_eq!(data.len(), 1024 * 1024);
    }
}

#[tokio::test]
async fn test_non_contiguous_indices() {
    let temp_dir = TempDir::new().unwrap();
    let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
    let namespace = StorageNamespace::new("test_gaps");

    // Write non-contiguous indices
    let entries = vec![
        (10, Bytes::from("entry 10")),
        (20, Bytes::from("entry 20")),
        (30, Bytes::from("entry 30")),
        (100, Bytes::from("entry 100")),
    ];
    storage.append(&namespace, entries).await.unwrap();

    // Verify bounds
    assert_eq!(storage.bounds(&namespace).await.unwrap(), Some((10, 100)));

    // Read various ranges
    let read_all = storage.read_range(&namespace, 0, 200).await.unwrap();
    assert_eq!(read_all.len(), 4);

    let read_middle = storage.read_range(&namespace, 15, 35).await.unwrap();
    assert_eq!(read_middle.len(), 2);
    assert_eq!(read_middle[0].0, 20);
    assert_eq!(read_middle[1].0, 30);

    let read_empty = storage.read_range(&namespace, 40, 90).await.unwrap();
    assert_eq!(read_empty.len(), 0);
}
