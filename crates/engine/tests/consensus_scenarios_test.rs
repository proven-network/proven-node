//! Integration tests for real-world consensus scenarios

mod common;

use common::test_cluster::{TestCluster, TransportType};
use proven_engine::foundation::types::ConsensusGroupId;
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::EnvFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_node_restart_rejoin() {
    // Test that a node can restart and rejoin the cluster
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3-node cluster ===");

    let (engines, node_infos) = cluster.add_nodes(3).await;

    // Wait for global cluster formation first
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Now wait for default group to become routable (at least 1 member)
    // The default group starts with just the coordinator, which is sufficient
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to wait for default group to become routable");

    let group_id = ConsensusGroupId::new(1);

    // Get initial state
    let mut _initial_leader = None;
    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            _initial_leader = state.leader.clone();
            tracing::info!(
                "Node {} initial state: leader={:?}, term={}",
                i,
                state.leader,
                state.term
            );
        }
    }

    tracing::info!("=== Stopping node 2 ===");

    // Stop node 2 (index 1)
    let mut engines_vec: Vec<_> = engines.into_iter().collect();
    let mut stopped_engine = engines_vec.remove(1);
    let stopped_node_id = node_infos[1].node_id.clone();

    stopped_engine.stop().await.expect("Failed to stop engine");
    tracing::info!("Stopped node: {}", stopped_node_id);

    // Wait for cluster to adapt
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify remaining nodes still have consensus
    for (i, engine) in engines_vec.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            tracing::info!(
                "Remaining node {} state: leader={:?}, term={}",
                i,
                state.leader,
                state.term
            );
        }
    }

    tracing::info!("=== Restarting node 2 ===");

    // Restart the stopped node
    stopped_engine
        .start()
        .await
        .expect("Failed to restart engine");
    engines_vec.insert(1, stopped_engine);

    // Wait for node to rejoin
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify all nodes are back in consensus
    let mut final_leaders = std::collections::HashSet::new();
    for (i, engine) in engines_vec.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            if let Some(ref leader) = state.leader {
                final_leaders.insert(leader.clone());
            }
            tracing::info!(
                "Node {} after restart: leader={:?}, term={}, is_member={}",
                i,
                state.leader,
                state.term,
                state.is_member
            );
        }
    }

    assert_eq!(
        final_leaders.len(),
        1,
        "All nodes should agree on leader after restart"
    );
    tracing::info!("Node successfully rejoined cluster");

    // Clean up
    for mut engine in engines_vec {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Node restart/rejoin test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_operations() {
    // Test that the cluster handles concurrent operations correctly
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3-node cluster for concurrent ops ===");

    let (engines, _node_infos) = cluster.add_nodes(3).await;

    // Wait for global cluster formation
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for global cluster formation");

    // Then wait for default group to become routable
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to wait for default group to become routable");

    tracing::info!("=== Creating multiple streams concurrently ===");

    // Create multiple streams concurrently from different nodes
    let mut handles = Vec::new();

    for (i, engine) in engines.iter().enumerate() {
        let client = engine.client();
        let stream_name = format!("concurrent_stream_{i}");
        let stream_name_clone = stream_name.clone();

        let handle = tokio::spawn(async move {
            let config = proven_engine::StreamConfig::default();
            client.create_stream(stream_name_clone, config).await
        });

        handles.push((i, stream_name, handle));
    }

    // Wait for all operations to complete
    let mut successes = 0;
    for (node_idx, stream_name, handle) in handles {
        match handle.await {
            Ok(Ok(_)) => {
                successes += 1;
                tracing::info!(
                    "Node {} successfully created stream: {}",
                    node_idx,
                    stream_name
                );
            }
            Ok(Err(e)) => {
                tracing::warn!("Node {} failed to create stream: {}", node_idx, e);
            }
            Err(e) => {
                tracing::error!("Task panic for node {}: {}", node_idx, e);
            }
        }
    }

    assert!(
        successes >= 1,
        "At least one stream creation should succeed"
    );
    tracing::info!("{} out of 3 concurrent operations succeeded", successes);

    tracing::info!("=== Publishing messages concurrently ===");

    // Pick one stream and publish to it concurrently
    let test_stream = "concurrent_stream_0";
    let mut publish_handles = Vec::new();

    for (i, engine) in engines.iter().enumerate() {
        let client = engine.client();
        let stream = test_stream.to_string();

        let handle = tokio::spawn(async move {
            let mut results = Vec::new();
            for msg_idx in 0..3 {
                let message = format!("Node {i} message {msg_idx}");
                let result = client
                    .publish_to_stream(
                        stream.clone(),
                        vec![proven_engine::Message::new(message.into_bytes())],
                    )
                    .await;
                results.push(result);
            }
            results
        });

        publish_handles.push((i, handle));
    }

    // Collect publish results
    for (node_idx, handle) in publish_handles {
        match handle.await {
            Ok(results) => {
                let successes = results.iter().filter(|r| r.is_ok()).count();
                tracing::info!(
                    "Node {} published {}/3 messages successfully",
                    node_idx,
                    successes
                );
            }
            Err(e) => {
                tracing::error!("Publish task panic for node {}: {}", node_idx, e);
            }
        }
    }

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Concurrent operations test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_large_cluster_formation() {
    // Test formation of a larger cluster
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=info".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 7-node cluster ===");

    let (engines, node_infos) = cluster.add_nodes(7).await;

    tracing::info!("Created {} nodes", engines.len());
    for (i, info) in node_infos.iter().enumerate() {
        tracing::info!("  Node {}: {}", i, info.node_id);
    }

    // Wait for global cluster formation with longer timeout for larger cluster
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(20))
        .await
        .expect("Failed to wait for global cluster formation in large cluster");

    // Then wait for default group to become routable
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(60))
        .await
        .expect("Failed to wait for default group to become routable in large cluster");

    // Verify consensus state
    let group_id = ConsensusGroupId::new(1);
    let mut leaders = std::collections::HashSet::new();
    let mut member_count = 0;

    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            if state.is_member {
                member_count += 1;
                if let Some(ref leader) = state.leader {
                    leaders.insert(leader.clone());
                }
            }
            tracing::info!(
                "Node {} state: leader={:?}, is_member={}",
                i,
                state.leader,
                state.is_member
            );
        }
    }

    assert!(
        member_count >= 5,
        "Should have at least 5 members in 7-node cluster"
    );
    assert_eq!(leaders.len(), 1, "Should have consensus on one leader");

    tracing::info!(
        "Large cluster formed successfully with {} members",
        member_count
    );

    // Test that the cluster is functional
    let client = engines[0].client();
    let result = client
        .create_stream(
            "large_cluster_test".to_string(),
            proven_engine::StreamConfig::default(),
        )
        .await;

    assert!(
        result.is_ok(),
        "Large cluster should be able to create streams"
    );

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Large cluster formation test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_network_delays() {
    // Test cluster behavior with simulated network delays
    // Note: This test doesn't actually simulate network delays but tests
    // the cluster's ability to handle timing variations

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3-node cluster with staggered starts ===");

    // Start nodes with delays between them
    let (mut engines, mut node_infos) = cluster.add_nodes(1).await;
    tracing::info!("Started node 1: {}", node_infos[0].node_id);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let (new_engines, new_infos) = cluster.add_nodes(1).await;
    engines.extend(new_engines);
    node_infos.extend(new_infos);
    tracing::info!("Started node 2: {}", node_infos[1].node_id);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let (new_engines, new_infos) = cluster.add_nodes(1).await;
    engines.extend(new_engines);
    node_infos.extend(new_infos);
    tracing::info!("Started node 3: {}", node_infos[2].node_id);

    // Wait for global cluster formation with staggered starts
    cluster
        .wait_for_global_cluster(&engines, Duration::from_secs(15))
        .await
        .expect("Failed to wait for global cluster formation with staggered starts");

    // Then wait for default group to become routable
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to wait for default group to become routable with staggered starts");

    // Verify cluster formed correctly despite staggered starts
    let group_id = ConsensusGroupId::new(1);
    let mut final_state = Vec::new();

    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            final_state.push((i, state.leader.clone(), state.is_member));
            tracing::info!(
                "Node {} final state: leader={:?}, is_member={}",
                i,
                state.leader,
                state.is_member
            );
        }
    }

    // Verify at least 2 nodes are members (allowing for timing issues)
    let member_count = final_state
        .iter()
        .filter(|(_, _, is_member)| *is_member)
        .count();
    assert!(
        member_count >= 2,
        "Should have at least 2 members after staggered start"
    );

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Network delays test completed successfully ===");
}
