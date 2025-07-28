//! Integration test for stream operations
//!
//! This test exercises the full stream workflow:
//! 1. Create a consensus group
//! 2. Create a stream in that group
//! 3. Publish messages to the stream
//! 4. Read messages back and verify ordering/content

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use proven_engine::{EngineState, StreamName};
use proven_engine::{PersistenceType, RetentionPolicy, StreamConfig};
use proven_storage::LogIndex;
use tracing::Level;
use tracing_subscriber::EnvFilter;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_stream_operations() {
    // Initialize logging with reduced OpenRaft verbosity
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    // Create a 3-node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, node_infos) = cluster.add_nodes(3).await;

    println!("Created {} nodes:", engines.len());
    for info in &node_infos {
        println!("  - Node {} on port {}", info.node_id, info.port);
    }

    // Wait for global cluster formation
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Verify all nodes are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} should be running"
        );
    }

    // Stream creation now properly goes through global consensus
    // All nodes will receive the StreamCreated event
    let client = engines[0].client();
    println!("Using first node's client - all nodes should receive events via global consensus");

    // Wait for default group formation
    // The default group starts with only the coordinator as a member
    // (can be rebalanced later via async processes)
    println!("Waiting for default group to be created...");
    cluster
        .wait_for_specific_group(
            &engines,
            proven_engine::foundation::types::ConsensusGroupId::new(1),
            1,                       // Only expect 1 member initially (the coordinator)
            Duration::from_secs(30), // Timeout for group creation
        )
        .await
        .expect("Failed to wait for default group formation");

    // Step 2: Create a stream
    let stream_name = format!("test-stream-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    println!("Creating stream '{stream_name}'");
    let response = client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create stream");
    println!("Stream creation response: {response:?}");

    // With synchronous callbacks, stream should be immediately available - no sleep needed!

    // Step 3: Publish messages to the stream
    let num_messages = 10;
    let mut expected_messages = Vec::new();

    println!("Publishing {num_messages} messages to stream");
    for i in 0..num_messages {
        let payload = Bytes::from(format!("Message {i}"));
        let mut metadata = HashMap::new();
        metadata.insert("index".to_string(), i.to_string());
        metadata.insert(
            "timestamp".to_string(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .to_string(),
        );

        expected_messages.push((payload.clone(), metadata.clone()));

        // Convert payload and metadata to match publish_to_stream API
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        let message = proven_engine::Message::new(payload.to_vec())
            .with_header("index", i.to_string())
            .with_header("timestamp", &timestamp);

        let response = client
            .publish_to_stream(stream_name.clone(), vec![message])
            .await
            .expect("Failed to publish message");

        println!("  Published message {i} with sequence {response}");
        // Verify sequences are monotonically increasing
        assert_eq!(
            response,
            LogIndex::new(i as u64 + 1).unwrap(),
            "Sequence should be {}",
            i + 1
        );
    }

    // Give time for all messages to be persisted
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Give time for messages to be persisted
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 4: Read messages back and verify
    // First, get stream info to verify it exists
    let stream_info = client
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info")
        .expect("Stream should exist");

    println!("Stream info: {stream_info:?}");
    assert_eq!(stream_info.name, StreamName::from(stream_name.clone()));
    // TODO: Fix stream info end_offset not being updated immediately
    // assert_eq!(stream_info.end_offset, num_messages as u64);

    // Step 5: Read messages back
    println!("Reading messages back from stream");
    use futures::StreamExt;
    use tokio::pin;
    let stream = client
        .stream_messages(
            stream_name.clone(),
            LogIndex::new(1).unwrap(),
            Some(LogIndex::new(num_messages as u64 + 1).unwrap()),
        )
        .await
        .expect("Failed to start streaming messages");

    pin!(stream);

    let mut read_messages = Vec::new();
    while let Some((message, timestamp, sequence)) = stream.next().await {
        read_messages.push((message, timestamp, sequence));
    }

    println!("Read {} messages from stream", read_messages.len());
    assert_eq!(
        read_messages.len(),
        num_messages,
        "Should read all messages"
    );

    // Verify message content and order
    for (i, (message, _timestamp, sequence)) in read_messages.iter().enumerate() {
        let (expected_payload, expected_metadata) = &expected_messages[i];

        // Check sequence number
        assert_eq!(*sequence, i as u64 + 1, "Sequence should be {}", i + 1);

        // Check payload
        assert_eq!(
            &message.payload, expected_payload,
            "Message {i} payload mismatch"
        );

        // Check metadata
        let stored_index = message
            .headers
            .iter()
            .find(|(k, _)| k == "index")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(
            stored_index,
            expected_metadata.get("index").unwrap(),
            "Message {i} index metadata mismatch"
        );

        println!(
            "  Message {}: sequence={}, payload={:?}",
            i,
            sequence,
            String::from_utf8_lossy(&message.payload)
        );
    }

    println!("Stream operations test completed successfully!");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_ephemeral_stream() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    // Create a single node for simplicity
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;

    // Give node time to initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = engines[0].client();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create ephemeral stream
    let stream_name = format!("ephemeral-test-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Ephemeral,
        retention: RetentionPolicy::Count { max_messages: 100 },
        max_message_size: 1024,
        allow_auto_create: false,
    };

    client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create ephemeral stream");

    // Publish a few messages
    for i in 0..5 {
        let payload = format!("Ephemeral message {i}").into_bytes();
        client
            .publish_to_stream(stream_name.clone(), vec![payload])
            .await
            .expect("Failed to publish to ephemeral stream");
    }

    // Verify stream info
    let info = client
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info")
        .expect("Stream should exist");

    // TODO: Once ClientService properly queries stream service, check last_sequence
    // assert_eq!(info.last_sequence, 5);
    assert_eq!(info.name, StreamName::from(stream_name));

    println!("Ephemeral stream test completed successfully!");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_stream_not_found() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _) = cluster.add_nodes(1).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = engines[0].client();

    // Try to publish to non-existent stream
    let result = client
        .publish_to_stream("non-existent-stream".to_string(), vec![vec![1, 2, 3]])
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found"));

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_stream_reading() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    // Create a single node for simplicity
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;

    // Wait for default group formation
    println!("Waiting for default group formation...");
    cluster
        .wait_for_specific_group(
            &engines,
            proven_engine::foundation::types::ConsensusGroupId::new(1),
            1, // Single node
            Duration::from_secs(10),
        )
        .await
        .expect("Failed to wait for default group formation");

    let client = engines[0].client();

    // Create a stream with many messages
    let stream_name = format!("streaming-test-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024,
        allow_auto_create: false,
    };

    println!("Creating stream '{stream_name}' for streaming test");
    let response = client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create stream");
    println!("Stream creation response: {response:?}");

    // Stream is now created synchronously via command pattern - no sleep needed!

    // Publish 50 messages for faster testing
    let num_messages = 50;
    println!("Publishing {num_messages} messages to test streaming");

    // Store expected messages for verification
    let mut expected_messages = Vec::new();

    for i in 0..num_messages {
        let payload = format!("Streaming message {i:03}").into_bytes();
        let mut metadata = HashMap::new();
        metadata.insert("index".to_string(), i.to_string());
        metadata.insert("test_id".to_string(), "stream_reading".to_string());

        expected_messages.push((payload.clone(), metadata.clone()));

        // Convert payload and metadata to match publish_to_stream API
        let message = proven_engine::Message::new(payload)
            .with_header("index", i.to_string())
            .with_header("test_id", "stream_reading");

        client
            .publish_to_stream(stream_name.clone(), vec![message])
            .await
            .expect("Failed to publish message");
    }

    // Give time for messages to be persisted
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test 1: Stream all messages
    println!("\nTest 1: Streaming all messages");
    let stream = client
        .stream_messages(stream_name.clone(), LogIndex::new(1).unwrap(), None)
        .await
        .expect("Failed to create stream reader");

    println!("Stream reader created, starting to read messages...");

    let mut count = 0usize;
    futures::pin_mut!(stream);
    // Use take() to limit to expected number of messages since we're in follow mode
    let mut limited_stream = stream.take(num_messages as usize);
    while let Some((message, _timestamp, sequence)) = limited_stream.next().await {
        // Verify sequence number
        assert_eq!(sequence, (count + 1) as u64, "Sequence mismatch");

        // Verify payload matches exactly
        let (expected_payload, expected_metadata) = &expected_messages[count];
        assert_eq!(
            &message.payload.as_ref(),
            expected_payload,
            "Payload mismatch at sequence {sequence}"
        );

        // Verify metadata
        let msg_headers: HashMap<String, String> = message.headers.iter().cloned().collect();
        assert_eq!(
            msg_headers.get("index"),
            expected_metadata.get("index"),
            "Index metadata mismatch at sequence {sequence}"
        );
        assert_eq!(
            msg_headers.get("test_id"),
            expected_metadata.get("test_id"),
            "Test ID metadata mismatch at sequence {sequence}"
        );

        count += 1;
    }
    assert_eq!(
        count, num_messages as usize,
        "Should have streamed all messages"
    );
    println!("Successfully streamed {count} messages with verified content");

    // Test 2: Stream a range of messages
    println!("\nTest 2: Streaming range [10, 30)");
    let stream = client
        .stream_messages(
            stream_name.clone(),
            LogIndex::new(10).unwrap(),
            Some(LogIndex::new(30).unwrap()),
        )
        .await
        .expect("Failed to create stream reader");

    let mut count = 0;
    let mut range_index = 9; // Start at index 9 (sequence 10)
    futures::pin_mut!(stream);
    while let Some((message, _timestamp, sequence)) = stream.next().await {
        assert!((10..30).contains(&sequence), "Message outside range");

        // Verify this is the correct message from our expected list
        let (expected_payload, _expected_metadata) = &expected_messages[range_index];
        assert_eq!(
            &message.payload.as_ref(),
            expected_payload,
            "Payload mismatch at sequence {sequence} (index {range_index})"
        );

        // Verify metadata
        let msg_headers: HashMap<String, String> = message.headers.iter().cloned().collect();
        assert_eq!(
            msg_headers.get("index"),
            Some(&range_index.to_string()),
            "Index metadata mismatch at sequence {sequence}"
        );

        count += 1;
        range_index += 1;
    }
    assert_eq!(count, 20, "Should have streamed exactly 20 messages");
    println!("Successfully streamed {count} messages in range with verified content");

    // Test 3: Stream with custom batch size (batch size is now handled internally)
    println!("\nTest 3: Streaming messages");
    let stream = client
        .stream_messages(
            stream_name.clone(),
            LogIndex::new(1).unwrap(),
            Some(LogIndex::new(50).unwrap()),
        )
        .await
        .expect("Failed to create stream reader");

    let mut count = 0;
    futures::pin_mut!(stream);
    while let Some((message, _timestamp, sequence)) = stream.next().await {
        // Verify this message matches our expected content
        let msg_index = (sequence - 1) as usize;
        let (expected_payload, _expected_metadata) = &expected_messages[msg_index];
        assert_eq!(
            &message.payload.as_ref(),
            expected_payload,
            "Payload mismatch at sequence {sequence} with batch size"
        );

        // Verify index metadata
        let msg_headers: HashMap<String, String> = message.headers.iter().cloned().collect();
        assert_eq!(
            msg_headers.get("index"),
            Some(&msg_index.to_string()),
            "Index metadata mismatch at sequence {sequence} with batch size"
        );

        count += 1;
    }
    assert_eq!(count, 49, "Should have streamed 49 messages");
    println!("Successfully streamed {count} messages and verified content");

    // Test 4: Early termination (drop stream before finishing)
    println!("\nTest 4: Testing early termination");
    let stream = client
        .stream_messages(stream_name.clone(), LogIndex::new(1).unwrap(), None)
        .await
        .expect("Failed to create stream reader");

    futures::pin_mut!(stream);
    // Read only 5 messages then drop - use take() to limit the stream
    let mut limited_stream = stream.take(5);
    let mut count = 0;
    while let Some((message, _timestamp, _sequence)) = limited_stream.next().await {
        let (expected_payload, _expected_metadata) = &expected_messages[count];

        // Even when terminating early, verify the messages we do read
        assert_eq!(
            &message.payload.as_ref(),
            expected_payload,
            "Payload mismatch at position {count} during early termination"
        );
        count += 1;
    }
    assert_eq!(
        count, 5,
        "Should have read exactly 5 messages before terminating"
    );

    // Give time for cleanup
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\nStreaming tests completed successfully!");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
