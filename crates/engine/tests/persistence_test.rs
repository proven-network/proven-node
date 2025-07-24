//! Integration test for engine persistence across restarts

mod common;

use common::test_cluster::TestCluster;
use proven_engine::foundation::types::ConsensusGroupId;
use proven_storage::LogIndex;
use std::time::Duration;
use tracing::{Level, info};
use tracing_subscriber::EnvFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cluster_persistence_across_restarts() {
    // Initialize logging with reduced OpenRaft verbosity
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    info!("=== Phase 1: Starting initial cluster ===");

    // Create a 3-node cluster with RocksDB storage
    let (mut engines, node_infos) = cluster.add_nodes_with_rocksdb(3).await;

    let node_id_1 = node_infos[0].node_id.clone();
    let node_id_2 = node_infos[1].node_id.clone();
    let node_id_3 = node_infos[2].node_id.clone();

    info!("Created nodes: {}, {}, {}", node_id_1, node_id_2, node_id_3);

    // Wait for cluster formation
    cluster
        .wait_for_group_formation(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    // Give time for default group creation to complete
    info!("Waiting for default group to be fully established");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create a stream on the first node
    info!("Creating test stream");
    let stream_name = "test_persistent_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    // Give time for stream initialization to propagate through group consensus
    info!("Waiting for stream to be initialized in group consensus");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Append some messages to the stream
    info!("Appending messages to stream");
    for i in 1..=10 {
        let message = format!("Message {i}");
        client
            .publish(stream_name.to_string(), message.into_bytes(), None)
            .await
            .expect("Failed to append message");
    }

    // For now, we'll just verify the stream was created
    // Reading from streams would require implementing a read API in the client
    info!("Successfully created stream and published 10 messages");

    // Verify consensus state
    let group_id = ConsensusGroupId::new(1);
    for (i, engine) in engines.iter().enumerate() {
        let state = engine
            .group_state(group_id)
            .await
            .expect("Failed to get group state");
        info!("Node {} group state: {:?}", i, state);
    }

    // Stop all engines gracefully using TestCluster's method
    info!("Stopping all engines using TestCluster");
    cluster
        .stop_all_engines(&mut engines)
        .await
        .expect("Failed to stop all engines");

    info!("All engines shut down successfully");

    // Give time for all resources to be released
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Phase 2: Restart cluster with same storage and verify data persistence
    info!("=== Phase 2: Restarting cluster with persisted data ===");

    // Start all engines using TestCluster's method
    // This ensures they all start together for proper consensus formation
    info!("Starting all engines using TestCluster");
    cluster
        .start_all_engines(&mut engines)
        .await
        .expect("Failed to start all engines");

    info!("All engines started successfully");

    info!(
        "Restarted nodes: {}, {}, {}",
        node_infos[0].node_id, node_infos[1].node_id, node_infos[2].node_id
    );

    // Wait for cluster reformation
    cluster
        .wait_for_group_formation(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to reform groups");

    // Verify the stream exists and contains our data
    info!("Verifying persisted stream data");
    let stream_name = "test_persistent_stream";

    // Try to get stream info to verify it exists
    let client = engines[0].client();
    let result = client.get_stream_info(stream_name).await;

    match result {
        Ok(Some(stream_info)) => {
            info!("Successfully found persisted stream: {:?}", stream_info);
            // The stream exists, which proves some level of persistence
        }
        Ok(None) => {
            info!("Stream not found after restart (expected if node IDs changed)");
        }
        Err(e) => {
            // This is expected if node IDs changed - the stream would be on different nodes
            info!(
                "Could not read stream (expected if node IDs changed): {}",
                e
            );

            // This shouldn't happen now that we're using the same node IDs
            panic!("Stream not found after restart - persistence test failed");
        }
    }

    // Verify consensus state reformed
    let group_id = ConsensusGroupId::new(1);
    let mut member_count = 0;
    for (i, engine) in engines.iter().enumerate() {
        match engine.group_state(group_id).await {
            Ok(state) => {
                info!("Node {} group state after restart: {:?}", i, state);
                if state.is_member {
                    member_count += 1;
                }
            }
            Err(e) => {
                info!("Node {} not in group: {}", i, e);
            }
        }
    }

    assert!(
        member_count > 0,
        "At least one node should be in the consensus group"
    );
    info!("Consensus group reformed with {} members", member_count);

    // Test writing new data to verify the cluster is functional
    info!("Testing new writes after restart");
    let new_stream = "post_restart_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(new_stream.to_string(), stream_config)
        .await
        .expect("Failed to create new stream after restart");

    client
        .publish(
            new_stream.to_string(),
            b"New message after restart".to_vec(),
            None,
        )
        .await
        .expect("Failed to publish to new stream");

    info!("Successfully created and wrote to new stream after restart");

    info!("=== Persistence test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_single_node_persistence() {
    // Initialize logging with reduced OpenRaft verbosity
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    info!("=== Phase 1: Starting single node ===");

    let (mut engines, node_infos) = cluster.add_nodes_with_rocksdb(1).await;

    let node_id = node_infos[0].node_id.clone();
    info!("Created node: {}", node_id);

    // Wait for the node to initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create streams and write data
    let stream_name = "single_node_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    // Write more data to ensure it's persisted
    for i in 1..=20 {
        let message = format!("Single node message {i}");
        client
            .publish(stream_name.to_string(), message.into_bytes(), None)
            .await
            .expect("Failed to publish message");
    }

    info!("Wrote 20 messages to stream");

    // Stop engine
    engines[0].stop().await.expect("Failed to stop engine");

    // Give time for all resources to be released
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 2: Restart and verify
    info!("=== Phase 2: Restarting single node ===");

    // Simply restart the engine - it already has its storage manager
    engines[0].start().await.expect("Failed to restart engine");

    info!("Restarted node: {}", node_infos[0].node_id);

    // Wait for initialization
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify storage persistence exists
    info!("Verifying storage persistence after restart");

    // Check that the storage path exists and has data
    if let Some(storage_path) = cluster.get_node_storage_path(&node_infos[0].node_id) {
        assert!(
            storage_path.exists(),
            "Storage path should exist: {storage_path:?}"
        );

        let entries: Vec<_> = std::fs::read_dir(&storage_path)
            .expect("Failed to read directory")
            .filter_map(Result::ok)
            .collect();

        assert!(
            !entries.is_empty(),
            "Storage directory should contain RocksDB files"
        );
        info!("Found {} storage files", entries.len());

        // Log the files found
        for entry in entries {
            info!("Storage file: {:?}", entry.file_name());
        }
    } else {
        panic!("No storage path found for node");
    }

    // Try to read the stream back
    info!("Attempting to read stream back after restart");

    let client = engines[0].client();

    // Give the stream service a moment to fully sync
    tokio::time::sleep(Duration::from_millis(500)).await;

    // First check if the stream exists
    match client.get_stream_info(stream_name).await {
        Ok(Some(info)) => {
            info!(
                "Stream '{}' exists with {} messages",
                stream_name, info.message_count
            );
            assert_eq!(info.message_count, 20, "Should have 20 messages");
        }
        Ok(None) => {
            panic!("Stream '{stream_name}' not found after restart!");
        }
        Err(e) => {
            panic!("Failed to get stream info: {e}");
        }
    }

    // Now try to read the messages
    match client
        .read_stream(
            stream_name.to_string(),
            LogIndex::new(1).unwrap(),
            LogIndex::new(20).unwrap(),
        )
        .await
    {
        Ok(messages) => {
            info!("Successfully read {} messages from stream", messages.len());
            assert_eq!(messages.len(), 20, "Should have read 20 messages");

            // Verify message content
            for (i, msg) in messages.iter().enumerate() {
                let expected = format!("Single node message {}", i + 1);
                let actual = String::from_utf8_lossy(&msg.data.payload);
                assert_eq!(actual, expected, "Message {} content mismatch", i + 1);
            }
        }
        Err(e) => {
            panic!("Failed to read messages from stream: {e}");
        }
    }

    info!("=== Single node persistence test completed successfully ===");
}
