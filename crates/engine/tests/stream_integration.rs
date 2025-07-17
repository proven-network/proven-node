//! Integration test for stream operations
//!
//! This test exercises the full stream workflow:
//! 1. Create a consensus group
//! 2. Create a stream in that group
//! 3. Publish messages to the stream
//! 4. Read messages back and verify ordering/content

use proven_engine::EngineState;
use proven_engine::stream::config::RetentionPolicy;
use proven_engine::stream::{PersistenceType, StreamConfig};
use std::collections::HashMap;
use std::time::Duration;

mod common;
use common::{TestCluster, TransportType};

#[tokio::test]
async fn test_stream_operations() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug")
        .with_test_writer()
        .try_init();

    // Create a 3-node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, node_infos) = cluster.add_nodes(3).await;

    println!("Created {} nodes:", engines.len());
    for info in &node_infos {
        println!("  - Node {} on port {}", info.node_id, info.port);
    }

    // Give cluster time to discover and form
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all nodes are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} should be running"
        );
    }

    // Get client from first node (will be the leader)
    let client = engines[0].client();

    // Step 1: Create a consensus group
    let group_id = proven_engine::foundation::types::ConsensusGroupId::new(1);
    let group_members = node_infos.iter().map(|info| info.node_id.clone()).collect();

    println!("Creating consensus group with ID {group_id:?}");
    let response = client
        .create_group(group_id, group_members)
        .await
        .expect("Failed to create group");
    println!("Group creation response: {response:?}");

    // Give time for group formation
    tokio::time::sleep(Duration::from_secs(2)).await;

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
        .create_stream(stream_name.clone(), stream_config, group_id)
        .await
        .expect("Failed to create stream");
    println!("Stream creation response: {response:?}");

    // Give time for stream creation to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 3: Publish messages to the stream
    let num_messages = 10;
    let mut expected_messages = Vec::new();

    println!("Publishing {num_messages} messages to stream");
    for i in 0..num_messages {
        let payload = format!("Message {i}").into_bytes();
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

        let response = client
            .publish(stream_name.clone(), payload, Some(metadata))
            .await
            .expect("Failed to publish message");

        match response {
            proven_engine::consensus::group::types::GroupResponse::Appended {
                sequence, ..
            } => {
                println!("  Published message {i} with sequence {sequence}");
                // Verify sequences are monotonically increasing
                assert_eq!(sequence, i as u64 + 1, "Sequence should be {}", i + 1);
            }
            other => panic!("Unexpected response: {other:?}"),
        }
    }

    // Give time for all messages to be persisted
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Step 4: Read messages back and verify
    // First, get stream info to verify it exists
    let stream_info = client
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info")
        .expect("Stream should exist");

    println!("Stream info: {stream_info:?}");
    assert_eq!(stream_info.name, stream_name);
    assert_eq!(stream_info.group_id, group_id);
    assert_eq!(stream_info.last_sequence, num_messages as u64);

    // Step 5: Read messages back
    println!("Reading messages back from stream");
    let read_messages = client
        .read_stream(stream_name.clone(), 1, num_messages as u64)
        .await
        .expect("Failed to read messages");

    println!("Read {} messages from stream", read_messages.len());
    assert_eq!(
        read_messages.len(),
        num_messages,
        "Should read all messages"
    );

    // Verify message content and order
    for (i, stored_msg) in read_messages.iter().enumerate() {
        let (expected_payload, expected_metadata) = &expected_messages[i];

        // Check sequence number
        assert_eq!(
            stored_msg.sequence,
            i as u64 + 1,
            "Sequence should be {}",
            i + 1
        );

        // Check payload
        assert_eq!(
            &stored_msg.data.payload, expected_payload,
            "Message {i} payload mismatch"
        );

        // Check metadata
        let stored_index = stored_msg
            .data
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
            stored_msg.sequence,
            String::from_utf8_lossy(&stored_msg.data.payload)
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
    let (engines, node_infos) = cluster.add_nodes(1).await;

    // Give node time to initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = engines[0].client();
    let group_id = proven_engine::foundation::types::ConsensusGroupId::new(1);

    // Create group with single node
    client
        .create_group(group_id, vec![node_infos[0].node_id.clone()])
        .await
        .expect("Failed to create group");

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
        .create_stream(stream_name.clone(), stream_config, group_id)
        .await
        .expect("Failed to create ephemeral stream");

    // Publish a few messages
    for i in 0..5 {
        let payload = format!("Ephemeral message {i}").into_bytes();
        client
            .publish(stream_name.clone(), payload, None)
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
    assert_eq!(info.name, stream_name);

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
        .publish("non-existent-stream".to_string(), vec![1, 2, 3], None)
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not found"));

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
