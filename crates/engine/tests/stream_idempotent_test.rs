//! Test that create_stream is idempotent
//!
//! This test verifies that calling create_stream multiple times
//! with the same stream name succeeds without errors.

use proven_engine::EngineState;
use proven_engine::{PersistenceType, RetentionPolicy, StreamConfig};
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::EnvFilter;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_create_stream_idempotent() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    // Create a 2-node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (mut engines, node_infos) = cluster.add_nodes(2).await;

    println!("Created {} nodes:", engines.len());
    for info in &node_infos {
        println!("  - Node {} on port {}", info.node_id, info.port);
    }

    // Wait for cluster formation
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Wait for default group to be created
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to wait for default group formation");

    // Verify all nodes are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} should be running"
        );
    }

    // Create a stream config
    let stream_name = format!("test-stream-{}", uuid::Uuid::new_v4());
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Forever,
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    // Test 1: Create stream from node 0
    println!("Creating stream '{stream_name}' from node 0");
    let client0 = engines[0].client();
    let response = client0
        .create_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create stream");
    println!("First create response: {response:?}");

    // Verify the stream was created
    match response {
        proven_engine::consensus::global::GlobalResponse::StreamCreated { .. } => {
            println!("Stream created successfully");
        }
        _ => panic!("Expected StreamCreated response, got: {response:?}"),
    }

    // Test 2: Create the same stream again from node 0 (should be idempotent)
    println!("Creating same stream '{stream_name}' again from node 0");
    let response2 = client0
        .create_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create stream (second time)");
    println!("Second create response: {response2:?}");

    // Should still get a success response due to idempotency
    match response2 {
        proven_engine::consensus::global::GlobalResponse::StreamCreated { .. } => {
            println!("Stream creation was idempotent - success!");
        }
        _ => panic!("Expected StreamCreated response, got: {response2:?}"),
    }

    // Test 3: Create the same stream from node 1 (different node, should also be idempotent)
    println!("Creating same stream '{stream_name}' from node 1");
    let client1 = engines[1].client();
    let response3 = client1
        .create_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create stream from node 1");
    println!("Third create response (from node 1): {response3:?}");

    // Should still get a success response
    match response3 {
        proven_engine::consensus::global::GlobalResponse::StreamCreated { .. } => {
            println!("Stream creation was idempotent across nodes - success!");
        }
        _ => panic!("Expected StreamCreated response, got: {response3:?}"),
    }

    // Test 4: Verify the stream exists and is usable
    println!("Verifying stream is usable by publishing a message");
    let message = b"Hello, idempotent world!";
    let publish_result = client0
        .publish(
            stream_name.clone(),
            message.to_vec(),
            Some(std::collections::HashMap::new()),
        )
        .await
        .expect("Failed to publish message");
    println!("Publish result: {publish_result:?}");

    // Test 5: Verify node 0 can see the stream (node 1 may not be in the group)
    let stream_info = client0
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info");

    assert!(stream_info.is_some(), "Node 0 should see the stream");
    println!("Node 0 sees stream: {stream_info:?}");

    // Node 1 might not see the stream if it's not in the group
    let stream_info1 = client1
        .get_stream_info(&stream_name)
        .await
        .expect("Failed to get stream info");

    if stream_info1.is_some() {
        println!("Node 1 also sees stream: {stream_info1:?}");
    } else {
        println!("Node 1 doesn't see stream (expected if not in group)");
    }

    println!("All idempotency tests passed!");

    // Shutdown gracefully
    for (i, engine) in engines.iter_mut().enumerate() {
        println!("Stopping engine {i}");
        engine.stop().await.expect("Failed to stop engine");
    }
}
