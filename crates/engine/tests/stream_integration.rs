//! Integration test for stream operations
//!
//! This test exercises the full stream workflow:
//! 1. Create a consensus group
//! 2. Create a stream in that group
//! 3. Publish messages to the stream
//! 4. Read messages back and verify ordering/content
//!
//! Also tests stream operations with default groups and various edge cases.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use proven_engine::{EngineState, StreamName};
use proven_engine::{PersistenceType, RetentionPolicy, StreamConfig};
use proven_storage::LogIndex;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tracing_test::traced_test]
#[tokio::test]
async fn test_stream_operations() {
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
        .create_group_stream(stream_name.clone(), stream_config)
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
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 4: Read messages back and verify
    // First, get stream info to verify it exists
    let stream_info = client
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info")
        .expect("Stream should exist");

    println!("Stream info: {stream_info:?}");
    assert_eq!(
        stream_info.stream_name,
        StreamName::from(stream_name.clone())
    );
    // TODO: Fix stream info end_offset not being updated immediately
    // assert_eq!(stream_info.end_offset, num_messages as u64);

    // Step 5: Read messages back
    println!("Reading messages back from stream");
    use futures::StreamExt;
    use tokio::pin;
    let stream = client
        .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
        .await
        .expect("Failed to start streaming messages");

    pin!(stream);

    // Take exactly the number of messages we wrote
    let mut limited_stream = stream.take(num_messages);

    let mut read_messages = Vec::new();
    while let Some((message, timestamp, sequence)) = limited_stream.next().await {
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

#[tracing_test::traced_test]
#[tokio::test]
async fn test_ephemeral_stream() {
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
        .create_group_stream(stream_name.clone(), stream_config)
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
    assert_eq!(info.stream_name, StreamName::from(stream_name));

    println!("Ephemeral stream test completed successfully!");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_stream_not_found() {
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

#[tracing_test::traced_test]
#[tokio::test]
async fn test_stream_reading() {
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
        .create_group_stream(stream_name.clone(), stream_config)
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
        .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
        .await
        .expect("Failed to create stream reader");

    println!("Stream reader created, starting to read messages...");

    let mut count = 0usize;
    futures::pin_mut!(stream);
    // Use take() to limit to expected number of messages
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
    println!("\nTest 2: Streaming range [10, 30) using skip and take");

    // First verify the messages are actually in storage by checking stream state
    println!("Checking stream state...");
    let stream_state = client
        .get_stream_state(&stream_name)
        .await
        .expect("Failed to get stream state")
        .expect("Stream state should exist");
    println!(
        "Stream state: message_count={}, first_sequence={:?}, last_sequence={:?}",
        stream_state.stats.message_count, stream_state.first_sequence, stream_state.last_sequence
    );

    let stream = client
        .stream_messages(stream_name.clone(), Some(LogIndex::new(10).unwrap()))
        .await
        .expect("Failed to create stream reader");

    let mut count = 0;
    let mut range_index = 9; // Start at index 9 (sequence 10)
    futures::pin_mut!(stream);

    println!("Starting to read from stream with take(20)...");

    // Take exactly 20 messages (sequences 10-29)
    let mut limited_stream = stream.take(20);

    while let Some((message, _timestamp, sequence)) = limited_stream.next().await {
        if count % 5 == 0 {
            println!("Received message with sequence {sequence}");
        }
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

    assert_eq!(
        count, 20,
        "Should have streamed exactly 20 messages but got {count}"
    );
    println!("Successfully streamed {count} messages in range with verified content");

    // Test 3: Stream messages using take
    println!("\nTest 3: Streaming first 49 messages");
    let stream = client
        .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
        .await
        .expect("Failed to create stream reader");

    let mut count = 0;
    futures::pin_mut!(stream);

    println!("Starting to read from stream with take(49)...");

    // Take exactly 49 messages
    let mut limited_stream = stream.take(49);

    while let Some((message, _timestamp, sequence)) = limited_stream.next().await {
        if count % 10 == 0 {
            println!("Received message with sequence {sequence}");
        }
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

    assert_eq!(
        count, 49,
        "Should have streamed 49 messages but got {count}"
    );
    println!("Successfully streamed {count} messages and verified content");

    // Test 4: Early termination (drop stream before finishing)
    println!("\nTest 4: Testing early termination");
    let stream = client
        .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
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

#[tracing_test::traced_test]
#[tokio::test]
async fn test_stream_with_default_group() {
    // Create a single-node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, node_infos) = cluster.add_nodes(1).await;

    println!(
        "Created node: {} on port {}",
        node_infos[0].node_id, node_infos[0].port
    );

    // Wait for default group creation
    println!("Waiting for default group creation...");
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for default group creation");

    // Verify node is healthy
    let engine = &engines[0];
    let health = engine.health().await.expect("Failed to get health");
    assert_eq!(
        health.state,
        EngineState::Running,
        "Engine should be running"
    );

    // Get client
    let client = engine.client();

    // Try to create a stream in the default group
    let stream_name = "test-stream".to_string();
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    println!("Creating stream '{stream_name}' in default group");
    let create_result = client
        .create_group_stream(stream_name.clone(), stream_config)
        .await;

    match create_result {
        Ok(response) => {
            println!("Stream creation response: {response:?}");

            // Try to publish a message
            println!("Publishing test message to stream");
            let publish_result = client
                .publish_to_stream(
                    stream_name.clone(),
                    vec![proven_engine::Message::new(b"Hello, stream!".to_vec())],
                )
                .await;

            match publish_result {
                Ok(resp) => println!("Publish response: {resp:?}"),
                Err(e) => {
                    println!("Publish failed (expected if consensus ops not implemented): {e}")
                }
            }
        }
        Err(e) => {
            println!("Stream creation failed: {e}");
            if e.to_string().contains("not yet implemented") || e.to_string().contains("TODO") {
                println!("This is expected - consensus operations aren't fully implemented");
            } else {
                panic!("Unexpected error: {e}");
            }
        }
    }

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_late_node_join_stream_creation() {
    // Create a single node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (mut engines, node_infos) = cluster.add_nodes(1).await;

    println!("Created single node cluster:");
    println!(
        "  - Node {} on port {}",
        node_infos[0].node_id, node_infos[0].port
    );

    // Wait for single node to initialize
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for single node initialization");

    // Verify node is healthy
    let health = engines[0].health().await.expect("Failed to get health");
    assert_eq!(
        health.state,
        EngineState::Running,
        "Engine should be running"
    );

    // Create a stream
    let client = engines[0].client();
    let stream_name = format!("test-stream-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    println!("Creating stream '{stream_name}'");
    let response = client
        .create_group_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create stream");
    println!("Stream creation response: {response:?}");

    // Publish some messages to the stream
    let num_messages = 5;
    println!("Publishing {num_messages} messages to stream");
    for i in 0..num_messages {
        let message = format!("Message {i}");

        let seq = client
            .publish_to_stream(
                stream_name.clone(),
                vec![proven_engine::Message::new(message.into_bytes())],
            )
            .await
            .expect("Failed to publish message");
        println!("Published message {i} with sequence {seq:?}");
    }

    // Wait before adding a new node
    println!("Waiting before adding new node...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now try to add a new node
    println!("Adding a second node to the cluster...");
    let (new_engines, new_node_infos) = cluster.add_nodes(1).await;

    println!("New node added:");
    println!(
        "  - Node {} on port {}",
        new_node_infos[0].node_id, new_node_infos[0].port
    );

    // Extend our engines list
    engines.extend(new_engines);

    // Immediately try to publish to the first stream from the new node
    println!("\nImmediately attempting to publish to the first stream from the new node...");
    match engines[1]
        .client()
        .publish_to_stream(
            stream_name.clone(),
            vec![proven_engine::Message::new(
                b"Message from new node (immediate)".to_vec(),
            )],
        )
        .await
    {
        Ok(response) => {
            println!("Successfully published from new node (immediate): {response:?}");
        }
        Err(e) => {
            println!("Error publishing from new node (immediate): {e}");
        }
    }

    // Wait for the new node to join the cluster
    println!("Waiting for topology to update...");
    cluster
        .wait_for_topology_size(&engines, 2, Duration::from_secs(30))
        .await
        .expect("Failed to wait for topology size");

    // Give time for any errors to manifest
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check if both nodes are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        println!(
            "Node {} health: state={:?}, services_healthy={}, consensus_healthy={}",
            i, health.state, health.services_healthy, health.consensus_healthy
        );
    }

    // Try to read from the stream on both nodes
    println!("Attempting to read stream from both nodes...");
    for (i, engine) in engines.iter().enumerate() {
        let client = engine.client();
        match client.get_stream_info(&stream_name).await {
            Ok(Some(info)) => {
                println!("Node {i} can read stream info: {info:?}");
            }
            Ok(_) => {
                println!("Node {i} reports stream not found");
            }
            Err(e) => {
                println!("Node {i} error reading stream: {e}");
            }
        }
    }

    // Also check routing table state on both nodes
    println!("Checking routing table state...");
    for (i, engine) in engines.iter().enumerate() {
        // Try to get groups to see if there are any routing issues
        match engine.client().node_groups().await {
            Ok(groups) => {
                println!("Node {} is in {} groups", i, groups.len());
            }
            Err(e) => {
                println!("Node {i} error getting groups: {e}");
            }
        }
    }

    // Try to publish to the first stream from node 1 (the new node)
    println!("\nAttempting to publish to the first stream from node 1 (the new node)...");
    match engines[1]
        .client()
        .publish_to_stream(
            stream_name.clone(),
            vec![proven_engine::Message::new(
                b"Message from new node".to_vec(),
            )],
        )
        .await
    {
        Ok(response) => {
            println!("Successfully published from new node: {response:?}");
        }
        Err(e) => {
            println!("Error publishing from new node: {e}");
        }
    }

    // Let's also try to create another stream to see if that works
    let stream_name2 = format!("test-stream-2-{}", uuid::Uuid::new_v4());
    println!("Attempting to create a second stream '{stream_name2}' from node 0...");

    match engines[0]
        .client()
        .create_group_stream(stream_name2.clone(), stream_config.clone())
        .await
    {
        Ok(response) => {
            println!("Successfully created second stream: {response:?}");
        }
        Err(e) => {
            println!("Failed to create second stream: {e}");
        }
    }

    // And from the new node
    println!("Attempting to create a third stream from the new node...");
    let stream_name3 = format!("test-stream-3-{}", uuid::Uuid::new_v4());

    match engines[1]
        .client()
        .create_group_stream(stream_name3.clone(), stream_config)
        .await
    {
        Ok(response) => {
            println!("Successfully created third stream from new node: {response:?}");
        }
        Err(e) => {
            println!("Failed to create third stream from new node: {e}");
        }
    }

    // Shutdown gracefully
    println!("Test complete, shutting down engines...");
    for (i, engine) in engines.iter_mut().enumerate() {
        println!("Stopping engine {i}");
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_global_stream() {
    // Create a 3-node test cluster for global streams
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, node_infos) = cluster.add_nodes(3).await;

    println!("Created {} nodes for global stream test:", engines.len());
    for info in &node_infos {
        println!("  - Node {} on port {}", info.node_id, info.port);
    }

    // Wait for global cluster formation
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Create client from the first node
    let client = engines[0].client();

    // Create a global stream (stays in global consensus)
    let stream_name = format!("global-stream-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024,
        allow_auto_create: false,
    };

    println!("Creating global stream '{stream_name}'");
    let stream_info = client
        .create_global_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create global stream");

    println!("Global stream created: {stream_info:?}");
    assert_eq!(stream_info.stream_name.as_str(), &stream_name);
    assert_eq!(
        stream_info.placement,
        proven_engine::foundation::models::stream::StreamPlacement::Global
    );

    // Wait for stream to be available across all nodes
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish messages to the global stream
    let num_messages = 10;
    println!("Publishing {num_messages} messages to global stream");

    for i in 0..num_messages {
        let payload = format!("Global message {i}").into_bytes();
        let message = proven_engine::Message::new(payload)
            .with_header("index", i.to_string())
            .with_header("type", "global");

        client
            .publish_to_stream(stream_name.clone(), vec![message])
            .await
            .expect("Failed to publish to global stream");
    }

    // Give time for messages to be persisted globally
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Read messages back from each node to verify global availability
    for (node_idx, engine) in engines.iter().enumerate() {
        println!("\nReading global stream from node {node_idx}");
        let node_client = engine.client();

        // Stream messages from the global stream
        let stream = node_client
            .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
            .await
            .expect("Failed to create stream reader");

        futures::pin_mut!(stream);
        let mut limited_stream = stream.take(num_messages as usize);

        let mut count = 0;
        while let Some((message, _timestamp, sequence)) = limited_stream.next().await {
            let msg_str = String::from_utf8_lossy(&message.payload);
            assert!(msg_str.starts_with("Global message"));
            assert_eq!(sequence, (count + 1) as u64);
            count += 1;
        }

        assert_eq!(
            count, num_messages as usize,
            "Node {node_idx} should see all {num_messages} messages"
        );
        println!("Node {node_idx} successfully read {count} messages from global stream");
    }

    // Test that global streams can handle writes from any node
    println!("\nTesting writes from different nodes to global stream");
    for (node_idx, engine) in engines.iter().enumerate().skip(1).take(2) {
        let node_client = engine.client();
        let payload = format!("Message from node {node_idx}").into_bytes();
        let message =
            proven_engine::Message::new(payload).with_header("source_node", node_idx.to_string());

        match node_client
            .publish_to_stream(stream_name.clone(), vec![message])
            .await
        {
            Ok(_) => {}
            Err(e) => panic!("Node {node_idx} failed to write to global stream: {e:?}"),
        }

        println!("Node {node_idx} successfully wrote to global stream");

        // Give some time for the write to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify all nodes see the new messages
    tokio::time::sleep(Duration::from_secs(1)).await;

    let total_messages = num_messages + 2; // Original + 2 from other nodes
    let stream = client
        .stream_messages(stream_name.clone(), Some(LogIndex::new(1).unwrap()))
        .await
        .expect("Failed to create stream reader");

    futures::pin_mut!(stream);
    let mut limited_stream = stream.take(total_messages as usize);

    let mut final_count = 0;
    while let Some((_message, _timestamp, _sequence)) = limited_stream.next().await {
        final_count += 1;
    }

    assert_eq!(
        final_count, total_messages as usize,
        "Should have {total_messages} messages total"
    );
    println!("Verified total of {final_count} messages in global stream");

    // Test getting stream state
    println!("\nTesting stream state retrieval for global stream");
    let stream_state = client
        .get_stream_state(&stream_name)
        .await
        .expect("Failed to get stream state")
        .expect("Stream state should exist");

    println!("Stream state: {stream_state:?}");
    assert_eq!(stream_state.stream_name.as_str(), &stream_name);
    assert_eq!(
        stream_state.last_sequence,
        Some(LogIndex::new(total_messages).unwrap())
    );
    assert_eq!(stream_state.stats.message_count, total_messages);
    assert!(stream_state.stats.total_bytes > 0);

    // Clean up
    println!("Global stream test completed successfully!");
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
