//! Demonstration of the new test helper methods
//!
//! This file shows how to use the new test helpers to simplify test setup

#[cfg(test)]
mod demo_tests {
    use proven_consensus::{
        test_multi_node_cluster, test_single_node_memory, test_single_node_rocksdb,
        test_single_node_tcp, test_websocket_node,
    };
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn demo_single_node_tcp() {
        println!("🧪 Demo: Single node with TCP transport using helper");

        // OLD WAY: 15+ lines of boilerplate
        // let signing_key = SigningKey::generate(&mut OsRng);
        // let governance = create_test_governance(port, &signing_key);
        // let attestor = Arc::new(MockAttestor::new());
        // let config = ConsensusConfig { ... };
        // let consensus = Consensus::new(config).await.unwrap();

        // NEW WAY: 1 line!
        let consensus = test_single_node_tcp(8080).await;

        // Start the node
        consensus.start().await.unwrap();

        // Test basic functionality
        assert!(!consensus.node_id().to_string().is_empty());

        // Clean shutdown
        consensus.shutdown().await.unwrap();

        println!("✅ Single node demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_single_node_auto_port() {
        println!("🧪 Demo: Single node with automatic port allocation");

        // NEW WAY: Auto-allocate port
        let (consensus, port) = test_single_node_memory().await;

        println!("📡 Node created on port: {}", port);

        consensus.start().await.unwrap();
        assert!(!consensus.is_leader()); // Not leader until cluster formation

        consensus.shutdown().await.unwrap();

        println!("✅ Auto-port demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_rocksdb_storage() {
        println!("🧪 Demo: Node with RocksDB storage");

        // NEW WAY: RocksDB with temp directory handled automatically
        let (consensus, _temp_dir) = test_single_node_rocksdb().await;

        consensus.start().await.unwrap();

        // Test that storage is working
        let global_state = consensus.global_state();
        let last_seq = global_state.last_sequence("test-stream").await;
        assert_eq!(last_seq, 0);

        consensus.shutdown().await.unwrap();
        // _temp_dir is automatically cleaned up when dropped

        println!("✅ RocksDB demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_websocket_transport() {
        println!("🧪 Demo: WebSocket transport with HTTP server");

        // NEW WAY: WebSocket with HTTP server setup automatically
        let (consensus, port, server_handle) = test_websocket_node().await;

        println!("🌐 WebSocket node with HTTP server on port: {}", port);

        consensus.start().await.unwrap();
        assert!(consensus.supports_http_integration());

        consensus.shutdown().await.unwrap();
        server_handle.abort(); // Clean up HTTP server

        println!("✅ WebSocket demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_multi_node_cluster() {
        println!("🧪 Demo: Multi-node cluster with shared governance");

        // OLD WAY: 50+ lines of boilerplate for multi-node setup
        // - Port allocation loops
        // - Key generation loops
        // - Shared governance creation
        // - Node configuration loops
        // - Manual cluster management

        // NEW WAY: 1 line + cluster management methods!
        let mut cluster = test_multi_node_cluster(3).await;

        println!("🚀 Created cluster with {} nodes", cluster.len());

        // Start all nodes
        cluster.start_all().await.unwrap();

        // Wait for cluster formation
        let formed = cluster.wait_for_cluster_formation(10).await;
        assert!(formed, "Cluster should form within 10 seconds");

        // Test cluster functionality
        let leader = cluster.get_leader();
        assert!(leader.is_some(), "Cluster should have a leader");

        println!("👑 Leader found: {}", leader.unwrap().node_id());

        // Clean shutdown
        cluster.shutdown_all().await;

        println!("✅ Multi-node cluster demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_cluster_operations() {
        println!("🧪 Demo: Cluster operations and testing");

        let mut cluster = test_multi_node_cluster(3).await;

        cluster.start_all().await.unwrap();
        cluster.wait_for_cluster_formation(10).await;

        // Test accessing individual nodes
        let node_0 = cluster.get_node(0).unwrap();
        let node_1 = cluster.get_node(1).unwrap();

        println!("📊 Node 0: {}", node_0.node_id());
        println!("📊 Node 1: {}", node_1.node_id());

        // Test cluster state consistency
        for (i, node) in cluster.nodes.iter().enumerate() {
            let cluster_size = node.cluster_size();
            println!("Node {} sees cluster size: {:?}", i, cluster_size);
            assert_eq!(
                cluster_size,
                Some(3),
                "All nodes should see cluster size of 3"
            );
        }

        cluster.shutdown_all().await;

        println!("✅ Cluster operations demo completed successfully");
    }

    #[tokio::test]
    #[traced_test]
    async fn demo_performance_comparison() {
        println!("🧪 Demo: Performance comparison - old vs new approach");

        let start = std::time::Instant::now();

        // Create 5 nodes using the new helper
        let mut cluster = test_multi_node_cluster(5).await;

        let setup_time = start.elapsed();
        println!("⚡ Cluster setup time: {:?}", setup_time);

        cluster.start_all().await.unwrap();
        let formed = cluster.wait_for_cluster_formation(15).await;

        let total_time = start.elapsed();
        println!("⚡ Total time to running cluster: {:?}", total_time);

        assert!(formed, "Large cluster should form successfully");
        assert_eq!(cluster.len(), 5);

        cluster.shutdown_all().await;

        println!("✅ Performance demo completed - significant time savings!");
        println!("   Old approach would require ~100+ lines of setup code");
        println!("   New approach: 1 line + helper methods");
    }
}
