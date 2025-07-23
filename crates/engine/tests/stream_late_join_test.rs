//! Integration test for late node join scenario
//!
//! This test exercises a specific failure case:
//! 1. Create a single node cluster
//! 2. Create a stream
//! 3. Publish to the stream
//! 4. Wait a bit
//! 5. Add a new node - should fail with "stream already created" error

use proven_engine::EngineState;
use proven_engine::{PersistenceType, RetentionPolicy, StreamConfig};
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::EnvFilter;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_late_node_join_stream_creation() {
    // Initialize logging with reduced OpenRaft verbosity
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("proven_engine::services::routing=trace".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

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
        .create_stream(stream_name.clone(), stream_config.clone())
        .await
        .expect("Failed to create stream");
    println!("Stream creation response: {response:?}");

    // Publish some messages to the stream
    let num_messages = 5;
    println!("Publishing {num_messages} messages to stream");
    for i in 0..num_messages {
        let message = format!("Message {i}");
        let headers = std::collections::HashMap::new();

        let seq = client
            .publish(stream_name.clone(), message.into_bytes(), Some(headers))
            .await
            .expect("Failed to publish message");
        println!("Published message {i} with sequence {seq:?}");
    }

    // Wait a bit (simulating the delay before adding a new node)
    println!("Waiting 5 seconds before adding new node...");
    tokio::time::sleep(Duration::from_secs(5)).await;

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
        .publish(
            stream_name.clone(),
            b"Message from new node (immediate)".to_vec(),
            None,
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

    // Give a very short time for any errors to manifest
    println!("Waiting briefly for potential errors to manifest...");
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
            Ok(None) => {
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
        match engine.node_groups().await {
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
        .publish(stream_name.clone(), b"Message from new node".to_vec(), None)
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
        .create_stream(stream_name2.clone(), stream_config.clone())
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
        .create_stream(stream_name3.clone(), stream_config)
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
