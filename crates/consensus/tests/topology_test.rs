use std::time::Duration;
use tracing_test::traced_test;

mod common;
use common::TestCluster;

#[tokio::test]
#[traced_test]
async fn test_topology_discovery() {
    println!("🧪 Testing topology discovery");

    // Create a 2-node cluster
    let mut cluster = TestCluster::new_with_tcp_and_memory(2).await;

    println!("Created cluster with {} nodes", cluster.len());

    // Before starting, check what each node sees in topology
    for (i, consensus) in cluster.consensus_instances.iter().enumerate() {
        println!("\n📍 Node {} ({})", i, consensus.node_id());

        // Check topology before start
        let topology = consensus.global_manager().topology();
        let peers = topology.get_all_peers().await;
        println!("  Peers in topology BEFORE start: {}", peers.len());
        for peer in &peers {
            println!("    - {} at {}", peer.node_id(), peer.origin());
        }
    }

    // Start only the first node
    println!("\n🚀 Starting node 0...");
    cluster.consensus_instances[0].start().await.unwrap();

    // Wait a bit
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check if it became a single-node cluster
    let node0_state = cluster.consensus_instances[0].cluster_state().await;
    println!("\nNode 0 state after start: {:?}", node0_state);
    println!(
        "Node 0 is_leader: {}",
        cluster.consensus_instances[0].is_leader()
    );

    // Now start the second node
    println!("\n🚀 Starting node 1...");
    cluster.consensus_instances[1].start().await.unwrap();

    // Wait for discovery
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check final states
    println!("\n📊 Final states:");
    for (i, consensus) in cluster.consensus_instances.iter().enumerate() {
        let state = consensus.cluster_state().await;
        println!(
            "Node {}: is_leader={}, cluster_size={:?}, state={:?}",
            i,
            consensus.is_leader(),
            consensus.cluster_size(),
            state
        );
    }

    cluster.shutdown_all().await;
}
