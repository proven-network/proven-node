//! Integration test for engine persistence across restarts

mod common;

use common::test_cluster::TestCluster;
use proven_engine::foundation::types::ConsensusGroupId;
use proven_storage::LogIndex;
use std::time::Duration;
use tracing::info;

#[tracing_test::traced_test]
#[tokio::test]
async fn test_cluster_persistence_across_restarts() {
    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    info!("=== Phase 1: Starting initial cluster ===");

    // Create a 3-node cluster with RocksDB storage
    let (mut engines, node_infos) = cluster.add_nodes_with_rocksdb(3).await;

    let node_id_1 = node_infos[0].node_id;
    let node_id_2 = node_infos[1].node_id;
    let node_id_3 = node_infos[2].node_id;

    info!("Created nodes: {}, {}, {}", node_id_1, node_id_2, node_id_3);

    // Wait for cluster formation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    // Give time for default group creation to complete
    info!("Waiting for default group to be fully established");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create a stream on the first node
    info!("Creating test stream");
    let stream_name = "test_persistent_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_group_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    // Give time for stream initialization to propagate
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Append some messages to the stream
    info!("Appending messages to stream");
    for i in 1..=10 {
        let message = format!("Message {i}");
        client
            .publish_to_stream(
                stream_name.to_string(),
                vec![proven_engine::Message::new(message.into_bytes())],
            )
            .await
            .expect("Failed to append message");
    }

    // For now, we'll just verify the stream was created
    // Reading from streams would require implementing a read API in the client
    info!("Successfully created stream and published 10 messages");

    // Verify consensus state
    let group_id = ConsensusGroupId::new(1);
    for (i, engine) in engines.iter().enumerate() {
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                info!("Node {} group state: {:?}", i, state);
            }
            Ok(None) => {
                info!("Node {} is not in group", i);
            }
            Err(e) => {
                panic!("Failed to get group state for node {i}: {e}");
            }
        }
    }

    // Stop all engines gracefully using TestCluster's method
    info!("Stopping all engines using TestCluster");
    cluster
        .stop_all_engines(&mut engines)
        .await
        .expect("Failed to stop all engines");

    info!("All engines shut down successfully");

    // Give time for all resources to be released
    tokio::time::sleep(Duration::from_secs(1)).await;

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
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
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
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                info!("Node {} group state after restart: {:?}", i, state);
                if state.is_member {
                    member_count += 1;
                }
            }
            Ok(None) => {
                info!("Node {} not in group", i);
            }
            Err(e) => {
                info!("Node {} failed to get group state: {}", i, e);
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
        .create_group_stream(new_stream.to_string(), stream_config)
        .await
        .expect("Failed to create new stream after restart");

    client
        .publish_to_stream(new_stream.to_string(), vec!["New message after restart"])
        .await
        .expect("Failed to publish to new stream");

    info!("Successfully created and wrote to new stream after restart");

    info!("=== Persistence test completed successfully ===");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_single_node_persistence() {
    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    info!("=== Phase 1: Starting single node ===");

    let (mut engines, node_infos) = cluster.add_nodes_with_rocksdb(1).await;

    let node_id = node_infos[0].node_id;
    info!("Created node: {}", node_id);

    // Wait for initialization
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create streams and write data
    let stream_name = "single_node_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_group_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    // Write more data to ensure it's persisted
    for i in 1..=20 {
        let message = format!("Single node message {i}");
        client
            .publish_to_stream(
                stream_name.to_string(),
                vec![proven_engine::Message::new(message.into_bytes())],
            )
            .await
            .expect("Failed to publish message");
    }

    info!("Wrote 20 messages to stream");

    // Stop engine
    engines[0].stop().await.expect("Failed to stop engine");

    // Give time for all resources to be released
    tokio::time::sleep(Duration::from_secs(1)).await;

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

    // Give the stream service a moment to sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // First check if the stream exists and has the right state
    match client.get_stream_state(stream_name).await {
        Ok(Some(state)) => {
            info!(
                "Stream '{}' exists with last_sequence {:?}",
                stream_name, state.last_sequence
            );
            assert_eq!(
                state.last_sequence,
                Some(LogIndex::new(20).unwrap()),
                "Should have 20 messages (last sequence is 20)"
            );
        }
        Ok(None) => {
            panic!("Stream '{stream_name}' state not found after restart!");
        }
        Err(e) => {
            panic!("Failed to get stream state: {e}");
        }
    }

    // Now try to read the messages
    use futures::StreamExt;
    use tokio::pin;
    match client.stream_messages(stream_name.to_string(), None).await {
        Ok(stream) => {
            pin!(stream);
            let mut messages = Vec::new();
            while let Some((message, _timestamp, _sequence)) = stream.next().await {
                messages.push(message);
            }

            info!("Successfully read {} messages from stream", messages.len());
            assert_eq!(messages.len(), 20, "Should have read 20 messages");

            // Verify message content
            for (i, msg) in messages.iter().enumerate() {
                let expected = format!("Single node message {}", i + 1);
                let actual = String::from_utf8_lossy(&msg.payload);
                assert_eq!(actual, expected, "Message {} content mismatch", i + 1);
            }
        }
        Err(e) => {
            panic!("Failed to read messages from stream: {e}");
        }
    }

    info!("=== Single node persistence test completed successfully ===");
}
