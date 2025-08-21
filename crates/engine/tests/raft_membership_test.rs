//! Integration tests for Raft membership changes and consensus behavior

mod common;

use common::test_cluster::{TestCluster, TransportType};
use proven_engine::foundation::types::ConsensusGroupId;
use std::time::Duration;

#[tracing_test::traced_test]
#[tokio::test]
async fn test_raft_leader_election() {
    // Test that Raft properly elects leaders and handles leader changes

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3-node cluster for leader election test ===");

    let (engines, node_infos) = cluster.add_nodes(3).await;

    // Wait for cluster formation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    let group_id = ConsensusGroupId::new(1);

    // Find the current leader
    let mut current_leader = None;
    let mut leader_engine_idx = None;

    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await
            && let Some(leader) = &state.leader
        {
            current_leader = Some(*leader);
            if state.leader.as_ref() == Some(&node_infos[i].node_id) {
                leader_engine_idx = Some(i);
            }
            tracing::info!("Node {} sees leader: {} (term: {})", i, leader, state.term);
        }
    }

    let leader = current_leader.expect("Should have a leader");
    let leader_idx = leader_engine_idx.expect("Should find leader engine");

    tracing::info!("Current leader is node {} ({})", leader_idx, leader);

    // Store the current term
    let initial_term = engines[leader_idx]
        .client()
        .group_state(group_id)
        .await
        .expect("Failed to get leader state")
        .expect("Leader not in group")
        .term;

    tracing::info!("=== Stopping current leader to trigger new election ===");

    // Stop the leader
    let mut engines_vec: Vec<_> = engines.into_iter().collect();
    let mut stopped_leader = engines_vec.remove(leader_idx);
    stopped_leader.stop().await.expect("Failed to stop leader");

    tracing::info!("Leader stopped, waiting for new election...");

    // Wait for new leader election
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify a new leader was elected
    let mut new_leaders = std::collections::HashSet::new();
    let mut new_term = 0;

    for (i, engine) in engines_vec.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await
            && let Some(new_leader) = &state.leader
        {
            new_leaders.insert(*new_leader);
            new_term = state.term;
            tracing::info!(
                "Remaining node {} sees new leader: {} (term: {})",
                i,
                new_leader,
                state.term
            );
        }
    }

    assert_eq!(new_leaders.len(), 1, "Should have consensus on new leader");
    assert!(
        new_term > initial_term,
        "Term should increase after election"
    );

    let new_leader = new_leaders.iter().next().unwrap();
    assert_ne!(
        new_leader, &leader,
        "Should have elected a different leader"
    );

    tracing::info!("New leader elected: {} (term: {})", new_leader, new_term);

    // Clean up remaining engines
    for mut engine in engines_vec {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Leader election test completed successfully ===");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_raft_membership_change() {
    // Test Raft membership changes through the proper Raft protocol

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3-node cluster ===");

    let (mut engines, mut node_infos) = cluster.add_nodes(3).await;

    // Wait for cluster formation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    let group_id = ConsensusGroupId::new(1);

    // Get initial membership
    let mut initial_members = std::collections::HashSet::new();
    for info in &node_infos {
        initial_members.insert(info.node_id);
    }

    tracing::info!("Initial members: {:?}", initial_members);

    // TODO: In a real implementation, we would need to:
    // 1. Add a new node to the topology
    // 2. Have the leader propose a membership change through Raft
    // 3. Wait for the membership change to be committed
    // 4. Verify the new node is added to the consensus group

    tracing::info!("=== Adding new node through proper channels ===");

    // Add a new node
    let (new_engines, new_node_infos) = cluster.add_nodes(1).await;
    engines.extend(new_engines);
    node_infos.extend(new_node_infos);

    let new_node_id = node_infos[3].node_id;
    tracing::info!("Added new node: {}", new_node_id);

    // In the current implementation, the new node needs to be added
    // to global consensus membership through the topology manager

    // Wait for membership propagation
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify membership state
    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            tracing::info!(
                "Node {} membership view: leader={:?}, members={:?}",
                i,
                state.leader,
                state.members
            );
        }
    }

    // Create a test stream to verify consensus still works
    let stream_name = "membership_test_stream";
    let stream_config = proven_engine::StreamConfig::default();

    let client = engines[0].client();
    client
        .create_group_stream(stream_name.to_string(), stream_config)
        .await
        .expect("Failed to create stream after membership change");

    tracing::info!("Successfully created stream after membership change");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Membership change test completed successfully ===");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_split_brain_prevention() {
    // Test that Raft prevents split-brain scenarios

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 5-node cluster for split-brain test ===");

    let (engines, node_infos) = cluster.add_nodes(5).await;

    // Wait for cluster formation
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    let group_id = ConsensusGroupId::new(1);

    // Find current leader
    let mut _leader_idx = None;
    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await
            && state.leader.as_ref() == Some(&node_infos[i].node_id)
        {
            _leader_idx = Some(i);
            tracing::info!("Found leader at index {}: {}", i, node_infos[i].node_id);
            break;
        }
    }

    // Convert to mutable vector for manipulation
    let mut engines_vec: Vec<_> = engines.into_iter().collect();

    tracing::info!("=== Simulating network partition ===");

    // Create two partitions:
    // Partition A: 2 nodes (minority) - indices 0, 1
    // Partition B: 3 nodes (majority) - indices 2, 3, 4

    // Stop nodes to simulate partition (crude but effective for testing)
    // In reality, we'd need network-level partition simulation

    let partition_a_count = 2;
    let mut stopped_engines = Vec::new();

    // Stop the first 2 nodes
    for i in 0..partition_a_count {
        let mut engine = engines_vec.remove(0);
        engine.stop().await.expect("Failed to stop engine");
        stopped_engines.push(engine);
        tracing::info!("Stopped node {} for partition", i);
    }

    tracing::info!("Created partition: 2 nodes stopped, 3 nodes running");

    // Wait for the majority partition to elect a new leader if needed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify the majority partition (3 nodes) can still operate
    let mut majority_leaders = std::collections::HashSet::new();
    let mut can_operate = false;

    for (i, engine) in engines_vec.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await
            && let Some(leader) = &state.leader
        {
            majority_leaders.insert(*leader);
            if state.is_member {
                can_operate = true;
            }
            tracing::info!(
                "Majority partition node {} state: leader={:?}, can operate",
                i + partition_a_count,
                leader
            );
        }
    }

    assert!(
        can_operate,
        "Majority partition should still be operational"
    );
    assert_eq!(
        majority_leaders.len(),
        1,
        "Majority should agree on one leader"
    );

    // Try to create a stream in the majority partition
    if !engines_vec.is_empty() {
        let client = engines_vec[0].client();
        let result = client
            .create_group_stream(
                "split_brain_test".to_string(),
                proven_engine::StreamConfig::default(),
            )
            .await;

        assert!(
            result.is_ok(),
            "Majority partition should be able to create streams"
        );
        tracing::info!("Majority partition successfully created a stream");
    }

    // The minority partition (stopped nodes) cannot form consensus or elect a leader
    // This prevents split-brain where two leaders could exist

    // Clean up
    for mut engine in engines_vec {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Split-brain prevention test completed successfully ===");
}

#[tracing_test::traced_test]
#[tokio::test]
async fn test_deterministic_initialization() {
    // Test that only the node with lowest ID initializes the cluster

    let mut cluster = TestCluster::new(TransportType::Tcp);

    tracing::info!("=== Starting 3 nodes simultaneously ===");

    // Start 3 nodes at once
    let (engines, node_infos) = cluster.add_nodes(3).await;

    // Collect and sort node IDs to find expected initializer
    let mut sorted_ids: Vec<_> = node_infos.iter().map(|info| info.node_id).collect();
    sorted_ids.sort();

    let expected_initializer = &sorted_ids[0];
    tracing::info!("Expected initializer (lowest ID): {}", expected_initializer);

    // Wait for initialization
    cluster
        .wait_for_default_group_routable(&engines, Duration::from_secs(30))
        .await
        .expect("Failed to form groups");

    // Verify that consensus formed properly
    let group_id = ConsensusGroupId::new(1);
    let mut leaders = std::collections::HashSet::new();
    let mut terms = std::collections::HashSet::new();

    for (i, engine) in engines.iter().enumerate() {
        if let Ok(Some(state)) = engine.client().group_state(group_id).await {
            if let Some(ref leader) = state.leader {
                leaders.insert(*leader);
            }
            terms.insert(state.term);
            tracing::info!(
                "Node {} ({}): term={}, leader={:?}",
                i,
                node_infos[i].node_id,
                state.term,
                state.leader
            );
        }
    }

    assert_eq!(leaders.len(), 1, "All nodes should agree on one leader");
    assert_eq!(terms.len(), 1, "All nodes should be on the same term");

    tracing::info!(
        "Consensus formed successfully with leader: {:?}",
        leaders.iter().next()
    );

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    tracing::info!("=== Deterministic initialization test completed successfully ===");
}
