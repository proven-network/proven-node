//! Test helper methods for Consensus and related types
//!
//! This module provides static helper methods to simplify test setup
//! and reduce boilerplate code across test files.

use crate::config::{
    ConsensusConfigBuilder, HierarchicalConsensusConfig, StorageConfig, TransportConfig,
};
use crate::{Consensus, Node};

use std::collections::HashSet;
use std::{sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_governance::{GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use rand::rngs::OsRng;
use tempfile;

/// Test cluster management helper
pub struct TestCluster {
    /// The consensus instances
    pub consensus_instances: Vec<Consensus<MockGovernance, MockAttestor>>,

    /// Shared governance instance
    pub governance: Arc<MockGovernance>,

    /// Nodes in the cluster
    pub nodes: Vec<Node>,

    /// Ports used by each node  
    pub ports: Vec<u16>,

    /// Signing keys for each node
    pub signing_keys: Vec<SigningKey>,
}

impl TestCluster {
    /// Private helper method to create a test cluster with specified configurations
    async fn new_internal(
        node_count: usize,
        transport_config_fn: impl Fn(u16) -> TransportConfig,
        storage_config_fn: impl Fn() -> StorageConfig,
    ) -> TestCluster {
        use crate::NodeId;

        // Generate ports and keys for all nodes
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();
        for _ in 0..node_count {
            ports.push(proven_util::port_allocator::allocate_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        // Create a single attestor for all nodes
        let attestor = Arc::new(MockAttestor::new());
        let version = Version::from_pcrs(attestor.pcrs_sync());

        // Create shared governance with all nodes
        let governance = Arc::new(MockGovernance::new(
            vec![], // Start with empty topology
            vec![version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        let mut consensus_instances = Vec::new();
        let mut nodes = Vec::new();
        let mut topology_managers = Vec::new();

        for (port, signing_key) in ports.iter().zip(signing_keys.iter()) {
            let node_id = NodeId::new(signing_key.verifying_key());
            let topology_manager =
                crate::topology::TopologyManager::new(governance.clone(), node_id);
            topology_managers.push(topology_manager);

            // Create node for governance
            let node = GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{}", port),
                public_key: signing_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };

            // Add node to governance
            governance
                .add_node(node.clone())
                .expect("Failed to add node to governance");

            nodes.push(Node::from(node));

            // Create consensus instance with Prometheus disabled for tests
            let mut hierarchical_config = HierarchicalConsensusConfig::default();
            hierarchical_config.monitoring.prometheus.enabled = false;

            let config = ConsensusConfigBuilder::new()
                .governance(governance.clone())
                .attestor(attestor.clone())
                .signing_key(signing_key.clone())
                .transport_config(transport_config_fn(*port))
                .storage_config(storage_config_fn())
                .hierarchical_config(hierarchical_config)
                .build()
                .expect("Failed to build test consensus config");

            let consensus = Consensus::new(config)
                .await
                .expect("Failed to create test consensus node");

            consensus_instances.push(consensus);
        }

        Self {
            consensus_instances,
            governance,
            nodes,
            ports,
            signing_keys,
        }
    }

    /// Create a test cluster with TCP transport and in-memory storage
    pub async fn new_with_tcp_and_memory(node_count: usize) -> TestCluster {
        Self::new_internal(
            node_count,
            |port| TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            },
            || StorageConfig::Memory,
        )
        .await
    }

    /// Create a test cluster with TCP transport and RocksDB storage
    pub async fn new_with_tcp_and_rocksdb(node_count: usize) -> TestCluster {
        Self::new_internal(
            node_count,
            |port| TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            },
            || StorageConfig::RocksDB {
                path: tempfile::tempdir().unwrap().path().to_path_buf(),
            },
        )
        .await
    }

    /// Create a test cluster with WebSocket transport and in-memory storage
    pub async fn new_with_websocket_and_memory(node_count: usize) -> TestCluster {
        Self::new_internal(
            node_count,
            |_port| TransportConfig::WebSocket,
            || StorageConfig::Memory,
        )
        .await
    }

    /// Create a test cluster with WebSocket transport and RocksDB storage
    pub async fn new_with_websocket_and_rocksdb(node_count: usize) -> TestCluster {
        Self::new_internal(
            node_count,
            |_port| TransportConfig::WebSocket,
            || StorageConfig::RocksDB {
                path: tempfile::tempdir().unwrap().path().to_path_buf(),
            },
        )
        .await
    }

    /// Start all nodes in the cluster
    pub async fn start_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (i, consensus) in self.consensus_instances.iter().enumerate() {
            println!("Starting test cluster node {} ({})", i, consensus.node_id());
            consensus.start().await?;
            // Brief delay to allow listener startup
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Additional time for cluster formation
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("✅ All {} test cluster nodes started", self.nodes.len());
        Ok(())
    }

    /// Wait for cluster formation with timeout
    pub async fn wait_for_cluster_formation(&self, timeout_secs: u64) -> bool {
        let start = std::time::Instant::now();

        while start.elapsed().as_secs() < timeout_secs {
            let mut leader_count = 0;
            let mut cluster_sizes = Vec::new();

            for consensus in &self.consensus_instances {
                if consensus.is_leader() {
                    leader_count += 1;
                }
                cluster_sizes.push(consensus.cluster_size());
            }

            // Check for exactly one leader and consistent cluster size
            if leader_count == 1 {
                let expected_size = Some(self.nodes.len());
                if cluster_sizes.iter().all(|&size| size == expected_size) {
                    println!(
                        "✅ Test cluster formation complete: {} nodes, 1 leader",
                        self.nodes.len()
                    );
                    return true;
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        println!(
            "❌ Test cluster formation timed out after {}s",
            timeout_secs
        );
        false
    }

    /// Shutdown all nodes gracefully
    pub async fn shutdown_all(&mut self) {
        for (i, consensus) in self.consensus_instances.iter().enumerate() {
            if let Err(e) = consensus.shutdown().await {
                println!("⚠️ Node {} shutdown error: {}", i, e);
            } else {
                println!("✅ Node {} shutdown successfully", i);
            }
        }
    }

    /// Get the current leader node, if any
    pub fn get_leader(&self) -> Option<&Consensus<MockGovernance, MockAttestor>> {
        self.consensus_instances
            .iter()
            .find(|consensus| consensus.is_leader())
    }

    /// Get a specific node by index
    pub fn get_consensus(&self, index: usize) -> Option<&Consensus<MockGovernance, MockAttestor>> {
        self.consensus_instances.get(index)
    }

    /// Get a specific node by index
    pub fn get_node(&self, index: usize) -> Option<&Node> {
        self.nodes.get(index)
    }

    /// Get the number of nodes in the cluster
    pub fn len(&self) -> usize {
        self.consensus_instances.len()
    }

    /// Check if the cluster is empty
    pub fn is_empty(&self) -> bool {
        self.consensus_instances.is_empty()
    }
}

/// Create a test TopologyManager
/// Returns a topology manager with a pre-configured 3-node cluster
pub async fn create_test_topology_manager() -> crate::topology::TopologyManager<MockGovernance> {
    let cluster = TestCluster::new_with_tcp_and_memory(3).await;
    let node_id = cluster.consensus_instances[0].node_id().clone();
    crate::topology::TopologyManager::new(cluster.governance, node_id)
}
