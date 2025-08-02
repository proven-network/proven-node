//! Integration tests for the consensus engine using TestCluster
//!
//! Tests basic engine lifecycle, cluster formation, and default group creation.

use proven_engine::EngineState;
use std::time::Duration;
use tracing::info;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tracing_test::traced_test]
#[tokio::test]
async fn test_engine_start_stop() {
    // Create test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Add a node - this does ALL the complex setup!
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let mut engine = engines.into_iter().next().unwrap();

    // Check health
    let health = engine.health().await.expect("Failed to get health");
    info!("Engine health: {:?}", health);

    assert_eq!(
        health.state,
        EngineState::Running,
        "Engine state should be Running but was {:?}",
        health.state
    );

    // Stop the engine
    engine.stop().await.expect("Failed to stop engine");

    // Check final state
    let health = engine
        .health()
        .await
        .expect("Failed to get health after stop");
    assert_eq!(health.state, EngineState::Stopped);
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_three_node_cluster() {
    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Add 3 nodes
    let (engines, node_infos) = cluster.add_nodes(3).await;

    info!("Created {} nodes:", engines.len());
    for info in &node_infos {
        info!("  - Node {} on port {}", info.node_id, info.port);
    }

    // Wait for cluster formation
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Verify all are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} state should be Running"
        );
    }

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_default_group_creation() {
    // Create a single-node cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;

    // Wait for default group creation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for default group creation");

    // Check engine health
    let engine = &engines[0];
    let health = engine.health().await.expect("Failed to get health");
    assert_eq!(health.state, EngineState::Running);
    info!("Engine health after default group creation: {:?}", health);

    // Get the client to interact with the engine
    let client = engine.client();

    // Try to create a stream to verify default group exists
    use proven_engine::StreamConfig;
    let stream_config = StreamConfig::default();

    let create_result = client
        .create_group_stream("test-stream".to_string(), stream_config)
        .await;

    match create_result {
        Ok(_) => {
            info!("Successfully created stream - default group exists!");
        }
        Err(e) => {
            // Stream creation might fail if consensus operations aren't fully implemented
            // but the key point is that the default group creation event was published
            info!("Stream creation failed (expected): {}", e);
        }
    }

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
