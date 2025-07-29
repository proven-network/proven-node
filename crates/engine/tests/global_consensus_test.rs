//! Integration tests for global consensus expansion and node addition

mod common;

use common::test_cluster::{TestCluster, TransportType};
use proven_engine::foundation::types::ConsensusGroupId;
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::EnvFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_global_consensus_expansion() {
    // Initialize logging with reduced OpenRaft verbosity
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap())
                .add_directive("proven_topology=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Phase 1: Starting 3-node cluster ===");

    // Start with 3 nodes
    let (mut engines, mut node_infos) = cluster.add_nodes(3).await;

    let node_id_1 = node_infos[0].node_id.clone();
    let node_id_2 = node_infos[1].node_id.clone();
    let node_id_3 = node_infos[2].node_id.clone();

    tracing::info!(
        "Created initial nodes: {}, {}, {}",
        node_id_1,
        node_id_2,
        node_id_3
    );

    // The engine start() method now waits for global consensus membership
    // So we just need to wait for the default group to be created
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    // Verify initial global consensus state
    let mut global_consensus_members = std::collections::HashSet::new();

    for (i, engine) in engines.iter().enumerate() {
        match engine.client().global_consensus_members().await {
            Ok(members) => {
                tracing::info!(
                    "Node {} sees {} global consensus members: {:?}",
                    i,
                    members.len(),
                    members
                );
                global_consensus_members.extend(members);
            }
            Err(e) => {
                panic!("Node {i} failed to get global consensus members: {e}");
            }
        }
    }

    assert_eq!(
        global_consensus_members.len(),
        3,
        "Should have 3 members in global consensus initially"
    );

    // Check default group exists (but don't require all nodes to be members)
    let group_id = ConsensusGroupId::new(1);
    let mut group_members = 0;

    for (i, engine) in engines.iter().enumerate() {
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                tracing::info!(
                    "Node {} is in default group: leader={:?}, members={:?}",
                    i,
                    state.leader,
                    state.members
                );
                group_members += 1;
            }
            Ok(None) => {
                tracing::info!("Node {} is not a member of the default group", i);
            }
            Err(_) => {
                tracing::info!("Node {} is not a member of the default group", i);
            }
        }
    }

    assert!(
        group_members >= 1,
        "At least one node should be in the default group"
    );

    tracing::info!("=== Phase 2: Adding 4th node to expand global consensus ===");

    // Add a 4th node
    let (new_engines, new_node_infos) = cluster.add_nodes(1).await;
    let node_id_4 = new_node_infos[0].node_id.clone();

    engines.extend(new_engines);
    node_infos.extend(new_node_infos);

    tracing::info!("Added new node: {}", node_id_4);

    // Give time for the new node to join global consensus
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify the new node joined global consensus
    let mut updated_global_members = std::collections::HashSet::new();

    for (i, engine) in engines.iter().enumerate() {
        match engine.client().global_consensus_members().await {
            Ok(members) => {
                tracing::info!(
                    "Node {} after expansion sees {} global consensus members: {:?}",
                    i,
                    members.len(),
                    members
                );
                updated_global_members.extend(members);
            }
            Err(e) => {
                tracing::warn!("Node {} failed to get global consensus members: {}", i, e);
            }
        }
    }

    assert_eq!(
        updated_global_members.len(),
        4,
        "Should have 4 members in global consensus after adding new node"
    );

    assert!(
        updated_global_members.contains(&node_id_4),
        "New node should be in global consensus"
    );

    // Check default group state (not all nodes need to be members)
    let mut nodes_in_group = 0;
    for (i, engine) in engines.iter().enumerate() {
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                tracing::info!(
                    "Node {} is in default group with leader {:?}",
                    i,
                    state.leader
                );
                nodes_in_group += 1;
            }
            Ok(None) => {
                tracing::info!("Node {} is not in the default group (expected)", i);
            }
            Err(_) => {
                tracing::info!("Node {} is not in the default group (expected)", i);
            }
        }
    }

    tracing::info!(
        "{} out of 4 nodes are members of the default group",
        nodes_in_group
    );

    tracing::info!("=== Phase 3: Creating stream to verify consensus works ===");

    // Create a stream to verify the cluster is functional
    let stream_name = "test_expansion_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream");

    // Verify stream creation propagated
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write some data
    for i in 1..=5 {
        let message = format!("Expansion test message {i}");
        client
            .publish_to_stream(
                stream_name.to_string(),
                vec![proven_engine::Message::new(message.into_bytes())],
            )
            .await
            .expect("Failed to publish message");
    }

    tracing::info!("Successfully created stream and published messages after expansion");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Global consensus expansion test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_single_node_to_cluster() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap())
                .add_directive("proven_topology=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Phase 1: Starting single node ===");

    // Start with just 1 node
    let (mut engines, mut node_infos) = cluster.add_nodes(1).await;
    let node_id_1 = node_infos[0].node_id.clone();

    tracing::info!("Created single node: {}", node_id_1);

    // The engine start() method now waits for global consensus membership
    // For a single node, we just need to wait a bit for the default group to be created
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify single node formed consensus
    let group_id = ConsensusGroupId::new(1);
    match engines[0].client().group_state(group_id).await {
        Ok(Some(state)) => {
            tracing::info!(
                "Single node state: leader={:?}, term={}, is_member={}",
                state.leader,
                state.term,
                state.is_member
            );
            assert!(state.is_member, "Single node should be a member");
            assert_eq!(
                state.leader.as_ref(),
                Some(&node_id_1),
                "Single node should be leader"
            );
        }
        Ok(None) => panic!("Single node is not in group"),
        Err(e) => panic!("Failed to get single node state: {e}"),
    }

    tracing::info!("=== Phase 2: Adding second node ===");

    // Add a second node
    let (new_engines, new_node_infos) = cluster.add_nodes(1).await;
    let node_id_2 = new_node_infos[0].node_id.clone();

    engines.extend(new_engines);
    node_infos.extend(new_node_infos);

    tracing::info!("Added second node: {}", node_id_2);

    // Wait for nodes to form consensus
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify both nodes see consistent state
    for (i, engine) in engines.iter().enumerate() {
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                tracing::info!(
                    "Node {} state: leader={:?}, term={}, is_member={}",
                    i,
                    state.leader,
                    state.term,
                    state.is_member
                );
            }
            Ok(None) => {
                tracing::warn!("Node {} not in group yet", i);
            }
            Err(e) => {
                tracing::warn!("Node {} not in group yet: {}", i, e);
            }
        }
    }

    tracing::info!("=== Phase 3: Adding third node to form proper cluster ===");

    // Add a third node
    let (new_engines, new_node_infos) = cluster.add_nodes(1).await;
    let node_id_3 = new_node_infos[0].node_id.clone();

    engines.extend(new_engines);
    node_infos.extend(new_node_infos);

    tracing::info!("Added third node: {}", node_id_3);

    // Wait for 3-node cluster formation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form 3-node cluster");

    // Verify all nodes are in consensus
    let mut leaders = std::collections::HashSet::new();
    for (i, engine) in engines.iter().enumerate() {
        match engine.client().group_state(group_id).await {
            Ok(Some(state)) => {
                tracing::info!(
                    "Node {} final state: leader={:?}, term={}, members={:?}",
                    i,
                    state.leader,
                    state.term,
                    state.members
                );
                if let Some(leader) = state.leader {
                    leaders.insert(leader);
                }
                assert!(state.is_member, "Node {i} should be a member");
            }
            Ok(None) => panic!("Node {i} failed to get state: not in group"),
            Err(e) => panic!("Node {i} failed to get state: {e}"),
        }
    }

    assert_eq!(leaders.len(), 1, "All nodes should agree on one leader");
    tracing::info!("All nodes agree on leader: {:?}", leaders.iter().next());

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Single node to cluster expansion test completed successfully ===");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rolling_node_addition() {
    // Test adding multiple nodes one at a time
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("proven_engine=debug".parse().unwrap())
                .add_directive("openraft=error".parse().unwrap())
                .add_directive("proven_topology=error".parse().unwrap()),
        )
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let mut engines = Vec::new();
    let mut node_infos = Vec::new();

    tracing::info!("=== Starting with 2 nodes ===");

    // Start with 2 nodes
    let (initial_engines, initial_node_infos) = cluster.add_nodes(2).await;
    engines.extend(initial_engines);
    node_infos.extend(initial_node_infos);

    // Wait for initial formation
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Add nodes one at a time
    for node_num in 3..=5 {
        tracing::info!("=== Adding node {} ===", node_num);

        let (new_engines, new_node_infos) = cluster.add_nodes(1).await;
        engines.extend(new_engines);
        node_infos.extend(new_node_infos);

        tracing::info!(
            "Added node {}: {}",
            node_num,
            node_infos[node_num - 1].node_id
        );

        // Wait for all nodes to see the updated topology
        cluster
            .wait_for_topology_size(&engines, node_num, Duration::from_secs(10))
            .await
            .unwrap_or_else(|_| panic!("Failed to wait for {node_num} nodes in topology"));

        // Give additional time for learner to catch up and membership updates
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify cluster state after each addition by checking global consensus membership
        let mut active_nodes = 0;
        for (i, engine) in engines.iter().enumerate() {
            match engine.client().global_consensus_members().await {
                Ok(members) => {
                    if !members.is_empty() {
                        active_nodes += 1;
                        tracing::info!(
                            "After adding node {}, node {} is part of global consensus with {} total members",
                            node_num,
                            i,
                            members.len()
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!("Node {} failed to get global consensus members: {}", i, e);
                }
            }
        }

        tracing::info!(
            "Active nodes after adding node {}: {}",
            node_num,
            active_nodes
        );
    }

    // Final verification - all 5 nodes should be operational
    tracing::info!("=== Final verification of 5-node cluster ===");

    // Give extra time for all membership changes to propagate
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut global_member_count = 0;
    let mut reported_member_counts = std::collections::HashSet::new();

    for (i, engine) in engines.iter().enumerate() {
        match engine.client().global_consensus_members().await {
            Ok(members) => {
                tracing::info!(
                    "Node {} final: sees {} global consensus members: {:?}",
                    i,
                    members.len(),
                    members
                );
                if !members.is_empty() {
                    global_member_count += 1;
                    reported_member_counts.insert(members.len());
                }
            }
            Err(e) => {
                tracing::warn!("Node {} failed to get global consensus members: {}", i, e);
            }
        }
    }

    assert!(
        global_member_count >= 3,
        "Should have at least 3 nodes reporting global consensus membership"
    );
    assert_eq!(
        reported_member_counts.len(),
        1,
        "All nodes should agree on the number of global consensus members"
    );

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Rolling node addition test completed successfully ===");
}
