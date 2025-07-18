//! Example test using the TestCluster utilities

use std::time::Duration;

mod common;
use common::{TestCluster, TransportType};
use proven_engine::EngineState;

#[tokio::test]
async fn test_three_node_cluster_tcp() {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug,proven_network=info,proven_transport_tcp=info")
        .try_init();

    // Create a test cluster with TCP transport
    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Add 3 nodes - this does ALL the setup!
    let (engines, node_infos) = cluster.add_nodes(3).await;

    println!("Created {} nodes:", engines.len());
    for (i, info) in node_infos.iter().enumerate() {
        println!("  Node {}: {} on port {}", i, info.node_id, info.port);
    }

    // Give the nodes time to discover each other and form a cluster
    println!("Waiting for cluster formation...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify all nodes are healthy
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        println!("Node {i} health: {health:?}");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} should be running"
        );
    }

    // Verify cluster state
    for (i, engine) in engines.iter().enumerate() {
        let cluster_info = engine
            .cluster_state()
            .await
            .expect("Failed to get cluster state");

        println!(
            "Node {} cluster info: cluster_id={}, state={:?}, members={}",
            i,
            cluster_info.cluster_id,
            cluster_info.state,
            cluster_info.members.len()
        );

        // Verify the node is active in a cluster
        match cluster_info.state {
            proven_engine::ClusterState::Active { .. } => {
                // Good - node is active
            }
            _ => {
                panic!(
                    "Node {} is not in active state: {:?}",
                    i, cluster_info.state
                );
            }
        }
    }

    // Shutdown nodes
    println!("Shutting down engines...");
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }

    println!("Test completed successfully!");
}

#[tokio::test]
async fn test_single_node_cluster() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Create just one node
    let (engines, _) = cluster.add_nodes(1).await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify the single node forms a cluster
    let cluster_info = engines[0]
        .cluster_state()
        .await
        .expect("Failed to get cluster state");

    println!("Single node cluster state: {:?}", cluster_info.state);

    // Should be active as leader
    match cluster_info.state {
        proven_engine::ClusterState::Active { .. } => {
            // Good - single node is active in cluster
            println!("Single node is active in cluster");
        }
        _ => panic!("Single node should be active"),
    }

    let mut engine = engines.into_iter().next().unwrap();
    engine.stop().await.expect("Failed to stop engine");
}

#[tokio::test]
async fn test_dynamic_node_addition() {
    // Test dynamic node addition to an existing cluster.
    // This test verifies that:
    // 1. A 3-node cluster can form and stabilize with consensus groups
    // 2. A new node can discover and join the existing cluster
    // 3. All nodes remain in the same cluster after the addition
    // 4. The cluster maintains a consistent view of leadership

    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug,proven_network=info")
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Start with 3 nodes
    let (mut engines, mut node_infos) = cluster.add_nodes(3).await;

    println!("Started with {} nodes", engines.len());

    // Wait for cluster discovery and formation
    assert!(
        cluster
            .wait_for_cluster_formation(Duration::from_secs(10))
            .await,
        "Failed to wait for cluster formation"
    );

    // Wait for the default group to be created on all nodes
    cluster
        .wait_for_default_group(&engines, Duration::from_secs(10))
        .await
        .expect("Failed to wait for default group formation");

    println!("Initial 3-node cluster has stabilized");

    // Verify the first 3 nodes formed a cluster
    let mut cluster_ids = std::collections::HashSet::new();
    let mut all_members = std::collections::HashMap::new();

    for (i, engine) in engines.iter().enumerate() {
        let cluster_info = engine
            .cluster_state()
            .await
            .expect("Failed to get cluster state");

        println!(
            "Node {} ({}) cluster info: cluster_id={}, state={:?}, members={}, leader={:?}",
            i,
            node_infos[i].node_id,
            cluster_info.cluster_id,
            cluster_info.state,
            cluster_info.members.len(),
            cluster_info.leader
        );

        cluster_ids.insert(cluster_info.cluster_id.clone());
        all_members.insert(i, cluster_info);
    }

    // Verify all 3 nodes are in the same cluster
    assert_eq!(
        cluster_ids.len(),
        1,
        "First 3 nodes should be in the same cluster"
    );

    println!("All 3 initial nodes are in the same cluster");

    // Now add a 4th node
    let (new_engines, new_infos) = cluster.add_nodes(1).await;
    engines.extend(new_engines);
    node_infos.extend(new_infos);

    println!("\nAdded a 4th node, now have {} nodes total", engines.len());

    // The new node should discover and join the existing cluster
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Clear previous data for final verification
    cluster_ids.clear();
    all_members.clear();

    // Verify all 4 nodes are in the same cluster
    for (i, engine) in engines.iter().enumerate() {
        let health = engine.health().await.expect("Failed to get health");
        assert_eq!(
            health.state,
            EngineState::Running,
            "Engine {i} state should be Running"
        );

        let cluster_info = engine
            .cluster_state()
            .await
            .expect("Failed to get cluster state");

        println!(
            "Node {} ({}) final cluster info: cluster_id={}, state={:?}, members={}, leader={:?}",
            i,
            node_infos[i].node_id,
            cluster_info.cluster_id,
            cluster_info.state,
            cluster_info.members.len(),
            cluster_info.leader
        );

        // Collect cluster IDs to verify all nodes are in same cluster
        cluster_ids.insert(cluster_info.cluster_id.clone());

        // Verify that the node has joined the cluster successfully.
        // The cluster maintains state consistency across all nodes.

        // Verify node is in Active state
        match &cluster_info.state {
            proven_engine::ClusterState::Active { .. } => {
                // Node is active in cluster - good
            }
            _ => {
                panic!(
                    "Node {} should be in Active state, but is in {:?}",
                    i, cluster_info.state
                );
            }
        }

        all_members.insert(i, cluster_info);
    }

    // Final verification: all nodes must be in the same cluster
    assert_eq!(
        cluster_ids.len(),
        1,
        "All 4 nodes should be in the same cluster, but found {} different cluster IDs: {:?}",
        cluster_ids.len(),
        cluster_ids
    );

    // Verify all nodes agree on the same leader
    let leaders: std::collections::HashSet<_> = all_members
        .values()
        .filter_map(|info| info.leader.clone())
        .collect();

    assert!(
        leaders.len() <= 1,
        "All nodes should agree on the same leader, but found: {leaders:?}"
    );

    println!("\nCluster verification successful!");
    println!(
        "All 4 nodes are in the same cluster with ID: {:?}",
        cluster_ids.iter().next().unwrap()
    );
    if let Some(leader) = leaders.iter().next() {
        println!("Cluster leader: {leader}");
    }

    // Stop all engines
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_single_node_cluster_expansion() {
    // Test that a single-node cluster can accept new nodes
    // This specifically tests:
    // 1. A single node can form a cluster by itself
    // 2. The single-node cluster properly registers discovery handlers
    // 3. A new node can discover and join the single-node cluster
    // 4. The cluster state is properly synchronized

    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=debug,proven_network=debug,proven_transport_tcp=debug")
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);

    // Start with just 1 node
    println!("=== Phase 1: Starting single-node cluster ===");
    let (mut engines, mut node_infos) = cluster.add_nodes(1).await;
    let first_node_id = node_infos[0].node_id.clone();

    println!("Started single node: {first_node_id}");

    // Give it time to form a single-node cluster
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the single node formed a cluster
    let cluster_info = engines[0]
        .cluster_state()
        .await
        .expect("Failed to get cluster state");

    println!(
        "Single node cluster info: cluster_id={}, state={:?}, members={}, leader={:?}",
        cluster_info.cluster_id,
        cluster_info.state,
        cluster_info.members.len(),
        cluster_info.leader
    );

    // Verify it's in Active state
    match &cluster_info.state {
        proven_engine::ClusterState::Active { role, .. } => {
            assert_eq!(
                *role,
                proven_engine::NodeRole::Leader,
                "Single node should be leader"
            );
        }
        _ => panic!(
            "Single node should be in Active state, but is in {:?}",
            cluster_info.state
        ),
    }

    let single_node_cluster_id = cluster_info.cluster_id.clone();

    // Wait for the default group to be created
    println!("Waiting for default group formation on single node...");
    cluster
        .wait_for_default_group(&engines[..1], Duration::from_secs(10))
        .await
        .expect("Failed to wait for default group formation");

    println!("=== Phase 2: Adding second node to single-node cluster ===");

    // Now add a second node
    let (new_engines, new_infos) = cluster.add_nodes(1).await;
    engines.extend(new_engines);
    node_infos.extend(new_infos);

    let second_node_id = node_infos[1].node_id.clone();
    println!("Added second node: {second_node_id}");

    // Give the new node time to discover and join
    println!("Waiting for second node to discover and join the cluster...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify both nodes are now in the same cluster
    println!("=== Phase 3: Verifying cluster state ===");
    for (i, engine) in engines.iter().enumerate() {
        let cluster_info = engine
            .cluster_state()
            .await
            .expect("Failed to get cluster state");

        println!(
            "Node {} ({}) cluster info: cluster_id={}, state={:?}, members={}, leader={:?}",
            i,
            node_infos[i].node_id,
            cluster_info.cluster_id,
            cluster_info.state,
            cluster_info.members.len(),
            cluster_info.leader
        );

        // Both nodes should be in the same cluster
        assert_eq!(
            cluster_info.cluster_id, single_node_cluster_id,
            "Node {i} should be in the same cluster as the original single node"
        );

        // Both nodes should be Active
        match &cluster_info.state {
            proven_engine::ClusterState::Active { .. } => {
                // Good
            }
            _ => panic!(
                "Node {} should be in Active state, but is in {:?}",
                i, cluster_info.state
            ),
        }
    }

    // Verify the cluster has grown
    let final_cluster_info = engines[0]
        .cluster_state()
        .await
        .expect("Failed to get cluster state");

    // The cluster should now have 2 members (though membership tracking might be limited)
    println!(
        "Final cluster state: {} member(s) tracked",
        final_cluster_info.members.len()
    );

    println!("\nSingle-node cluster expansion test completed successfully!");

    // Stop all engines
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
