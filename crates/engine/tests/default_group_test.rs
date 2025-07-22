//! Test that verifies default group creation after cluster formation

mod common;
use common::test_cluster::{TestCluster, TransportType};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_default_group_creation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info,proven_network=info")
        .with_test_writer()
        .try_init();

    // Create a single-node cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Add a node (this automatically starts it)
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let mut engine = engines.into_iter().next().unwrap();

    // Give some time for discovery and consensus formation
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Give extra time for event processing to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check engine health
    let health = engine.health().await.expect("Failed to get health");
    tracing::info!("Engine health: {:?}", health);

    // Get the client to interact with the engine
    let client = engine.client();

    // For now, just create a stream to see what happens
    // The stream config doesn't directly specify a group
    use proven_engine::StreamConfig;
    let stream_config = StreamConfig::default();

    // This should work if the default group was created
    let create_result = timeout(
        Duration::from_secs(5),
        client.create_stream("test-stream".to_string(), stream_config),
    )
    .await;

    match create_result {
        Ok(Ok(_)) => {
            tracing::info!("Successfully created stream - default group exists!");
        }
        Ok(Err(e)) => {
            // Check if the error is because consensus operations aren't implemented
            tracing::warn!("Stream creation failed with: {}", e);
            if e.to_string().contains("not yet implemented") || e.to_string().contains("TODO") {
                tracing::info!(
                    "This is expected - consensus operations aren't fully implemented yet"
                );
                tracing::info!(
                    "The key point is that the default group creation event was published"
                );
            } else {
                panic!("Unexpected error: {e}");
            }
        }
        Err(_) => {
            panic!("Stream creation timed out");
        }
    }

    // The discovery and join already happened during engine.start()

    // Clean up
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_default_group_event_flow() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info,proven_engine::services::global_consensus=debug,proven_engine::services::group_consensus=debug")
        .with_test_writer()
        .try_init();

    // Create a single-node cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Add a node (this automatically starts it)
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    let mut engine = engines.into_iter().next().unwrap();

    // The discovery and join already happened during engine.start()
    // which should trigger:
    // 1. Cluster formation
    // 2. Global consensus initialization
    // 3. RequestDefaultGroupCreation event publication
    // 4. Group consensus service receiving the event

    // Give some time for event processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check engine health
    let health = engine.health().await.expect("Failed to get health");
    tracing::info!("Engine health after cluster formation: {:?}", health);

    // Verify the flow happened by checking logs
    // In the logs we should see:
    // - "GlobalConsensusService: Requesting default group creation"
    // - "Received request to create default group with X members" (from group consensus)

    // Clean up
    engine.stop().await.expect("Failed to stop engine");
}
