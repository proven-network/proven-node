//! Basic test to verify RocksDB storage integration works

mod common;

use common::test_cluster::TestCluster;
use std::time::Duration;
use tracing::{Level, info};
use tracing_subscriber::EnvFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rocksdb_storage_basic() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap()),
        )
        .try_init();

    info!("=== Starting node with RocksDB storage ===");
    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    let (engines, node_infos) = cluster.add_nodes_with_rocksdb(1).await;

    let node_id = node_infos[0].node_id.clone();
    info!("Created node: {}", node_id);

    // Wait for the node to initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create a stream
    let stream_name = "test_rocksdb_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    info!("Created stream: {}", stream_name);

    // Publish some messages
    for i in 1..=5 {
        let message = format!("RocksDB test message {i}");
        client
            .publish_to_stream(
                stream_name.to_string(),
                vec![proven_engine::Message::new(message.into_bytes())],
            )
            .await
            .expect("Failed to publish message");
    }

    info!("Published 5 messages");

    // Verify the stream exists
    let stream_info = client
        .get_stream_info(stream_name)
        .await
        .expect("Failed to get stream info");

    assert!(stream_info.is_some(), "Stream should exist");
    info!("Stream verified: {:?}", stream_info);

    // Check storage files were created
    if let Some(storage_path) = cluster.get_node_storage_path(&node_id) {
        assert!(storage_path.exists(), "Storage path should exist");

        let entries: Vec<_> = std::fs::read_dir(&storage_path)
            .expect("Failed to read directory")
            .filter_map(Result::ok)
            .collect();

        assert!(
            !entries.is_empty(),
            "Storage directory should contain RocksDB files"
        );
        info!("Found {} RocksDB files", entries.len());

        // Log some of the files
        for (i, entry) in entries.iter().take(5).enumerate() {
            info!("  File {}: {:?}", i + 1, entry.file_name());
        }
    } else {
        panic!("No storage path found for node");
    }

    info!("Test completed successfully - RocksDB storage is working");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_rocksdb_multi_node() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap()),
        )
        .try_init();

    info!("=== Starting 3-node cluster with RocksDB storage ===");
    let mut cluster = TestCluster::new(common::test_cluster::TransportType::Tcp);

    let (engines, node_infos) = cluster.add_nodes_with_rocksdb(3).await;

    info!("Created nodes:");
    for (i, info) in node_infos.iter().enumerate() {
        info!("  Node {}: {}", i + 1, info.node_id);
    }

    // Wait for cluster formation
    cluster
        .wait_for_group_formation(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    info!("Cluster formed successfully");

    // Create a stream on the first node
    let stream_name = "multi_node_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    info!("Created stream on first node");

    // Publish messages from different nodes
    for (node_idx, engine) in engines.iter().enumerate() {
        let client = engine.client();
        for i in 1..=3 {
            let message = format!("Node {} message {}", node_idx + 1, i);
            client
                .publish_to_stream(
                    stream_name.to_string(),
                    vec![proven_engine::Message::new(message.into_bytes())],
                )
                .await
                .expect("Failed to publish message");
        }
        info!("Published 3 messages from node {}", node_idx + 1);
    }

    // Verify each node has its own RocksDB storage
    for (i, node_info) in node_infos.iter().enumerate() {
        if let Some(storage_path) = cluster.get_node_storage_path(&node_info.node_id) {
            assert!(
                storage_path.exists(),
                "Storage path for node {i} should exist"
            );

            let entry_count = std::fs::read_dir(&storage_path)
                .expect("Failed to read directory")
                .count();

            assert!(entry_count > 0, "Node {i} should have RocksDB files");
            info!("Node {i} has {entry_count} storage files");
        } else {
            panic!("No storage path found for node {i}");
        }
    }

    info!("Multi-node RocksDB test completed successfully");
}
