use bytes::Bytes;
use proven_storage::{LogIndex, LogStorage, LogStorageStreaming, StorageNamespace};
use proven_storage_memory::MemoryStorage;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

// Helper function to create LogIndex
fn nz(n: u64) -> LogIndex {
    LogIndex::new(n).expect("test indices should be non-zero")
}

#[tokio::test]
async fn test_basic_operations() {
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("test_basic");

    // Test initial bounds (should be None)
    assert_eq!(storage.bounds(&namespace).await.unwrap(), None);

    // Test append (sequential)
    let entries = Arc::new(vec![
        Bytes::from("entry 1"),
        Bytes::from("entry 2"),
        Bytes::from("entry 3"),
    ]);
    let last_seq = storage.append(&namespace, entries).await.unwrap();
    assert_eq!(last_seq, nz(3));

    // Test bounds after append
    assert_eq!(
        storage.bounds(&namespace).await.unwrap(),
        Some((nz(1), nz(3)))
    );

    // Read entries
    let read = storage.read_range(&namespace, nz(1), nz(4)).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read[0], (nz(1), Bytes::from("entry 1")));
    assert_eq!(read[1], (nz(2), Bytes::from("entry 2")));
    assert_eq!(read[2], (nz(3), Bytes::from("entry 3")));

    // Read partial range
    let partial = storage.read_range(&namespace, nz(2), nz(3)).await.unwrap();
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0], (nz(2), Bytes::from("entry 2")));
}

#[tokio::test]
async fn test_multiple_namespaces() {
    let storage = MemoryStorage::new();

    let ns1 = StorageNamespace::new("namespace1");
    let ns2 = StorageNamespace::new("namespace2");
    let ns3 = StorageNamespace::new("namespace3");

    // Write to different namespaces
    storage
        .append(&ns1, Arc::new(vec![Bytes::from("ns1 data")]))
        .await
        .unwrap();

    // Use put_at for specific indices in other namespaces
    storage
        .put_at(&ns2, vec![(nz(100), Arc::new(Bytes::from("ns2 data")))])
        .await
        .unwrap();
    storage
        .put_at(&ns3, vec![(nz(1000), Arc::new(Bytes::from("ns3 data")))])
        .await
        .unwrap();

    // Verify isolation between namespaces
    assert_eq!(storage.bounds(&ns1).await.unwrap(), Some((nz(1), nz(1))));
    assert_eq!(
        storage.bounds(&ns2).await.unwrap(),
        Some((nz(100), nz(100)))
    );
    assert_eq!(
        storage.bounds(&ns3).await.unwrap(),
        Some((nz(1000), nz(1000)))
    );

    let read1 = storage.read_range(&ns1, nz(1), nz(2000)).await.unwrap();
    assert_eq!(read1.len(), 1);
    assert_eq!(read1[0], (nz(1), Bytes::from("ns1 data")));

    let read2 = storage.read_range(&ns2, nz(1), nz(2000)).await.unwrap();
    assert_eq!(read2.len(), 1);
    assert_eq!(read2[0], (nz(100), Bytes::from("ns2 data")));
}

#[tokio::test]
async fn test_engine_like_namespaces() {
    // Test with namespace patterns similar to what the engine uses
    let storage = MemoryStorage::new();

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
        "stream_test-stream-12345",
    ];

    for ns_name in namespaces.iter() {
        let ns = StorageNamespace::new(*ns_name);
        let data = format!("data for {ns_name}");
        storage
            .append(&ns, Arc::new(vec![Bytes::from(data)]))
            .await
            .unwrap();
    }

    // Verify we can read from a complex namespace
    let ns = StorageNamespace::new("stream_group-1_store_698df805-5623-4e45-97b4-9501b583d444");
    let read = storage.read_range(&ns, nz(1), nz(100)).await.unwrap();
    assert_eq!(read.len(), 1);
    assert!(read[0].1.starts_with(b"data for"));

    // Test a stream namespace
    let stream_ns = StorageNamespace::new("stream_test-stream-12345");
    let read = storage
        .read_range(&stream_ns, nz(1), nz(100))
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert!(read[0].1.starts_with(b"data for stream_"));
}

#[tokio::test]
async fn test_truncate_after() {
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("test_truncate");

    // Append entries sequentially
    let entries = Arc::new(vec![
        Bytes::from("entry 1"),
        Bytes::from("entry 2"),
        Bytes::from("entry 3"),
        Bytes::from("entry 4"),
        Bytes::from("entry 5"),
    ]);
    storage.append(&namespace, entries).await.unwrap();

    // Truncate after index 3
    storage.truncate_after(&namespace, nz(3)).await.unwrap();

    // Verify bounds updated
    assert_eq!(
        storage.bounds(&namespace).await.unwrap(),
        Some((nz(1), nz(3)))
    );

    // Verify entries 4 and 5 are gone
    let read = storage.read_range(&namespace, nz(1), nz(6)).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read.last().unwrap().0, nz(3));
}

#[tokio::test]
async fn test_compact_before() {
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("test_compact");

    // Append entries sequentially
    let entries = Arc::new(vec![
        Bytes::from("entry 1"),
        Bytes::from("entry 2"),
        Bytes::from("entry 3"),
        Bytes::from("entry 4"),
        Bytes::from("entry 5"),
    ]);
    storage.append(&namespace, entries).await.unwrap();

    // Compact before index 3 (should remove 1 and 2)
    storage.compact_before(&namespace, nz(2)).await.unwrap();

    // Verify bounds updated
    assert_eq!(
        storage.bounds(&namespace).await.unwrap(),
        Some((nz(3), nz(5)))
    );

    // Verify entries 1 and 2 are gone
    let read = storage.read_range(&namespace, nz(1), nz(6)).await.unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(read.first().unwrap().0, nz(3));
}

#[tokio::test]
async fn test_concurrent_access() {
    let storage = Arc::new(MemoryStorage::new());

    let mut tasks = JoinSet::new();

    // Spawn multiple tasks writing to different namespaces concurrently
    for i in 0..10 {
        let storage_clone = storage.clone();
        tasks.spawn(async move {
            let namespace = StorageNamespace::new(format!("concurrent_{i}"));
            let entries: Arc<Vec<Bytes>> = Arc::new(
                (1..=100)
                    .map(|j| Bytes::from(format!("task {i} entry {j}")))
                    .collect(),
            );

            storage_clone.append(&namespace, entries).await.unwrap();

            // Verify writes
            let bounds = storage_clone.bounds(&namespace).await.unwrap();
            assert_eq!(bounds, Some((nz(1), nz(100))));
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    // Verify all namespaces have correct data
    for i in 0..10 {
        let namespace = StorageNamespace::new(format!("concurrent_{i}"));
        let read = storage
            .read_range(&namespace, nz(1), nz(101))
            .await
            .unwrap();
        assert_eq!(read.len(), 100);
        assert_eq!(read[0], (nz(1), Bytes::from(format!("task {i} entry 1"))));
        assert_eq!(
            read[99],
            (nz(100), Bytes::from(format!("task {i} entry 100")))
        );
    }
}

#[tokio::test]
async fn test_large_entries() {
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("test_large");

    // Create large entries (1MB each)
    let large_data = vec![0u8; 1024 * 1024];
    let entries: Arc<Vec<Bytes>> =
        Arc::new((1..=10).map(|_| Bytes::from(large_data.clone())).collect());

    storage.append(&namespace, entries).await.unwrap();

    // Read back and verify
    let read = storage.read_range(&namespace, nz(1), nz(11)).await.unwrap();
    assert_eq!(read.len(), 10);
    for (_, data) in read {
        assert_eq!(data.len(), 1024 * 1024);
    }
}

#[tokio::test]
async fn test_non_contiguous_indices() {
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("test_gaps");

    // Write non-contiguous indices - use put_at for specific positions
    let entries = vec![
        (nz(10), Arc::new(Bytes::from("entry 10"))),
        (nz(20), Arc::new(Bytes::from("entry 20"))),
        (nz(30), Arc::new(Bytes::from("entry 30"))),
        (nz(100), Arc::new(Bytes::from("entry 100"))),
    ];
    storage.put_at(&namespace, entries).await.unwrap();

    // Verify bounds
    assert_eq!(
        storage.bounds(&namespace).await.unwrap(),
        Some((nz(10), nz(100)))
    );

    // Read various ranges
    let read_all = storage
        .read_range(&namespace, nz(1), nz(200))
        .await
        .unwrap();
    assert_eq!(read_all.len(), 4);

    let read_middle = storage
        .read_range(&namespace, nz(15), nz(35))
        .await
        .unwrap();
    assert_eq!(read_middle.len(), 2);
    assert_eq!(read_middle[0].0, nz(20));
    assert_eq!(read_middle[1].0, nz(30));

    let read_empty = storage
        .read_range(&namespace, nz(40), nz(90))
        .await
        .unwrap();
    assert_eq!(read_empty.len(), 0);
}

#[tokio::test]
async fn test_streaming_follow_mode() {
    let storage = Arc::new(MemoryStorage::new());
    let namespace = StorageNamespace::new("test_follow");

    // Start with some initial entries
    let entries = Arc::new(vec![Bytes::from("initial 1"), Bytes::from("initial 2")]);
    storage.append(&namespace, entries).await.unwrap();

    // Start streaming in follow mode (no end bound)
    let storage_clone = storage.clone();
    let namespace_clone = namespace.clone();
    let stream_handle = tokio::spawn(async move {
        let mut stream = storage_clone
            .stream_range(&namespace_clone, Some(nz(1)))
            .await
            .unwrap();

        let mut results = Vec::new();

        // Read exactly 5 entries (2 initial + 3 new)
        for _ in 0..5 {
            if let Some(item) = stream.next().await {
                results.push(item.unwrap());
            }
        }

        results
    });

    // Give the stream a chance to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Append more entries while streaming
    let new_entries = Arc::new(vec![
        Bytes::from("new 1"),
        Bytes::from("new 2"),
        Bytes::from("new 3"),
    ]);
    storage.append(&namespace, new_entries).await.unwrap();

    // Get the results
    let results = stream_handle.await.unwrap();

    assert_eq!(results.len(), 5);
    assert_eq!(results[0], (nz(1), Bytes::from("initial 1")));
    assert_eq!(results[1], (nz(2), Bytes::from("initial 2")));
    assert_eq!(results[2], (nz(3), Bytes::from("new 1")));
    assert_eq!(results[3], (nz(4), Bytes::from("new 2")));
    assert_eq!(results[4], (nz(5), Bytes::from("new 3")));
}

#[tokio::test]
async fn test_stream_namespace_pattern() {
    // This test specifically tests the stream namespace pattern used by the engine
    let storage = MemoryStorage::new();
    let stream_name = "test-stream-698df805-5623-4e45-97b4-9501b583d444";
    let namespace = StorageNamespace::new(format!("stream_{stream_name}"));

    // Simulate what happens in the engine
    let entries = Arc::new(vec![
        Bytes::from("message 1"),
        Bytes::from("message 2"),
        Bytes::from("message 3"),
    ]);

    let last_seq = storage.append(&namespace, entries).await.unwrap();
    assert_eq!(last_seq, nz(3));

    // Verify we can read the data back
    let read = storage.read_range(&namespace, nz(1), nz(4)).await.unwrap();
    assert_eq!(read.len(), 3);

    // Test streaming with bounded range (like in the engine tests)
    let mut stream = storage.stream_range(&namespace, Some(nz(1))).await.unwrap();

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let (index, data) = result.unwrap();
        match index.get() {
            1 => assert_eq!(data, Bytes::from("message 1")),
            2 => assert_eq!(data, Bytes::from("message 2")),
            3 => assert_eq!(data, Bytes::from("message 3")),
            _ => panic!("Unexpected index"),
        }
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_delayed_read_pattern() {
    // This test simulates the pattern where we write, wait, then read
    // which is what happens in the engine tests
    let storage = Arc::new(MemoryStorage::new());

    let stream_name = "test-stream-delayed";
    let namespace = StorageNamespace::new(format!("stream_{stream_name}"));

    // Phase 1: Write messages
    let serialized_entries: Vec<Bytes> = (1..=10)
        .map(|i| {
            let data = format!("Message {i}");
            Bytes::from(data)
        })
        .collect();

    let entries = Arc::new(serialized_entries);
    let last_seq = storage.append(&namespace, entries).await.unwrap();
    assert_eq!(last_seq, nz(10));

    // Verify data is there with read_range
    let immediate_read = storage.read_range(&namespace, nz(1), nz(11)).await.unwrap();
    assert_eq!(immediate_read.len(), 10);

    // Phase 2: Add a delay like in engine tests
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Phase 3: Try to stream the data
    let mut stream = storage.stream_range(&namespace, None).await.unwrap();

    let mut messages = Vec::new();
    while let Some(result) = stream.next().await {
        let (index, data) = result.unwrap();
        messages.push((index, data));
    }

    assert_eq!(messages.len(), 10, "Should stream all 10 messages");

    // Verify message content
    for (i, (index, data)) in messages.iter().enumerate() {
        assert_eq!(index.get(), (i + 1) as u64);
        let expected = format!("Message {}", i + 1);
        assert_eq!(data.as_ref(), expected.as_bytes());
    }
}

#[tokio::test]
async fn test_concurrent_write_and_stream() {
    // Test pattern where streaming starts before all writes are complete
    let storage = Arc::new(MemoryStorage::new());
    let namespace = StorageNamespace::new("stream_concurrent_test");

    // Write initial batch
    let initial_entries = Arc::new(vec![
        Bytes::from("msg_1"),
        Bytes::from("msg_2"),
        Bytes::from("msg_3"),
    ]);
    storage.append(&namespace, initial_entries).await.unwrap();

    // Start streaming with bounded range
    let storage_clone = storage.clone();
    let namespace_clone = namespace.clone();

    let stream_task = tokio::spawn(async move {
        let mut stream = storage_clone
            .stream_range(&namespace_clone, None) // Expect 6 messages total
            .await
            .unwrap();

        let mut collected = Vec::new();
        while let Some(result) = stream.next().await {
            let (idx, data) = result.unwrap();
            collected.push((idx, data));
        }
        collected
    });

    // Give stream a chance to start
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Write more entries while streaming
    let more_entries = Arc::new(vec![
        Bytes::from("msg_4"),
        Bytes::from("msg_5"),
        Bytes::from("msg_6"),
    ]);
    storage.append(&namespace, more_entries).await.unwrap();

    // Get results
    let results = stream_task.await.unwrap();
    assert_eq!(results.len(), 6, "Should receive all 6 messages");

    // Verify sequences
    for (i, (idx, _)) in results.iter().enumerate() {
        assert_eq!(idx.get(), (i + 1) as u64);
    }
}

#[tokio::test]
async fn test_cloned_storage_streaming() {
    // Test that streaming works correctly with cloned storage instances
    let storage = MemoryStorage::new();
    let namespace = StorageNamespace::new("stream_clone_test");

    // Write with original storage
    let entries = Arc::new(vec![
        Bytes::from("msg_1"),
        Bytes::from("msg_2"),
        Bytes::from("msg_3"),
        Bytes::from("msg_4"),
        Bytes::from("msg_5"),
    ]);
    storage.append(&namespace, entries).await.unwrap();

    // Clone the storage multiple times
    let storage_clone1 = storage.clone();
    let storage_clone2 = storage.clone();
    let storage_clone3 = storage.clone();

    // Test 1: Read with clone1
    let read_result = storage_clone1
        .read_range(&namespace, nz(1), nz(6))
        .await
        .unwrap();
    assert_eq!(read_result.len(), 5);

    // Test 2: Stream with clone2
    let mut stream = storage_clone2.stream_range(&namespace, None).await.unwrap();

    let mut stream_results = Vec::new();
    while let Some(result) = stream.next().await {
        stream_results.push(result.unwrap());
    }
    assert_eq!(stream_results.len(), 5);

    // Test 3: Append more with clone3 and verify visibility
    let more_entries = Arc::new(vec![Bytes::from("msg_6"), Bytes::from("msg_7")]);
    storage_clone3
        .append(&namespace, more_entries)
        .await
        .unwrap();

    // Test 4: Original storage should see all entries
    let final_read = storage.read_range(&namespace, nz(1), nz(8)).await.unwrap();
    assert_eq!(final_read.len(), 7);

    // Test 5: Stream from a fresh clone should see all entries
    let storage_clone4 = storage.clone();
    let mut stream2 = storage_clone4.stream_range(&namespace, None).await.unwrap();

    let mut all_results = Vec::new();
    while let Some(result) = stream2.next().await {
        all_results.push(result.unwrap());
    }
    assert_eq!(all_results.len(), 7);
}

#[tokio::test]
async fn test_storage_manager_pattern() {
    // This test mimics how the engine uses StorageManager with stream_storage()
    use proven_storage::StorageManager;

    let base_storage = MemoryStorage::new();
    let storage_manager = Arc::new(StorageManager::new(base_storage));

    // Get stream storage like the engine does
    let stream_storage = storage_manager.stream_storage();
    let namespace = StorageNamespace::new("stream_test-stream-12345");

    // Write entries
    let entries = Arc::new(vec![
        Bytes::from("entry_1"),
        Bytes::from("entry_2"),
        Bytes::from("entry_3"),
    ]);

    let last_seq = stream_storage.append(&namespace, entries).await.unwrap();
    assert_eq!(last_seq, nz(3));

    // Now try to stream - using the same storage reference
    let mut stream = LogStorageStreaming::stream_range(&stream_storage, &namespace, None)
        .await
        .unwrap();

    let mut count = 0;
    while let Some(result) = stream.next().await {
        let (_idx, _data) = result.unwrap();
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_engine_exact_flow() {
    // This test mimics the EXACT flow of the engine stream tests
    use proven_storage::StorageManager;

    // Create storage exactly like engine does
    let base_storage = MemoryStorage::new();
    let storage_manager = Arc::new(StorageManager::new(base_storage));

    // The stream name from the test
    let stream_name = "test-stream-698df805-5623-4e45-97b4-9501b583d444";
    let namespace = StorageNamespace::new(format!("stream_{stream_name}"));

    // Step 1: Persist messages (what PersistMessages handler does)
    {
        let storage = storage_manager.stream_storage();

        // Simulate 10 serialized message entries
        let entries: Vec<Bytes> = (1..=10)
            .map(|i| {
                // In the real engine, this would be serialize_entry(message, timestamp, sequence)
                let data = format!(
                    "{{\"payload\":\"Message {}\",\"timestamp\":{},\"sequence\":{}}}",
                    i,
                    1234567890 + i,
                    i
                );
                Bytes::from(data)
            })
            .collect();

        let last_seq = storage.append(&namespace, Arc::new(entries)).await.unwrap();
        assert_eq!(last_seq, nz(10));

        // Verify data was written
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(10))));
    }

    // Step 2: Add delay like in engine test
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Step 3: Read messages back (what StreamMessages handler does)
    {
        let storage = storage_manager.stream_storage();

        // Try both read_range and stream_range
        let read_result = storage.read_range(&namespace, nz(1), nz(11)).await.unwrap();
        assert_eq!(
            read_result.len(),
            10,
            "read_range should return 10 messages"
        );

        // Now try streaming
        let mut stream = LogStorageStreaming::stream_range(&storage, &namespace, None)
            .await
            .unwrap();

        let mut streamed_count = 0;
        while let Some(result) = stream.next().await {
            let (_idx, _data) = result.unwrap();
            streamed_count += 1;
        }

        assert_eq!(streamed_count, 10, "stream_range should return 10 messages");
    }
}
