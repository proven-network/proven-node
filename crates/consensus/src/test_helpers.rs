//! Test helper methods for Consensus and related types
//!
//! This module provides static helper methods to simplify test setup
//! and reduce boilerplate code across test files.

#[cfg(any(test, feature = "test-helpers"))]
use std::collections::HashSet;
use std::{sync::Arc, time::Duration};

use crate::Consensus;
use ed25519_dalek::SigningKey;
use proven_attestation::Attestor;
#[cfg(any(test, feature = "test-helpers"))]
use proven_attestation_mock::MockAttestor;
use proven_governance::Governance;
#[cfg(any(test, feature = "test-helpers"))]
use proven_governance::{GovernanceNode, Version};
#[cfg(any(test, feature = "test-helpers"))]
use proven_governance_mock::MockGovernance;
#[cfg(any(test, feature = "test-helpers"))]
use rand::rngs::OsRng;
#[cfg(any(test, feature = "test-helpers"))]
use tempfile;

#[cfg(any(test, feature = "test-helpers"))]
use crate::config::{ConsensusConfigBuilder, StorageConfig, TransportConfig};

/// Test cluster management helper
pub struct TestCluster<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// All consensus nodes in the cluster
    pub nodes: Vec<Consensus<G, A>>,
    /// Ports used by each node  
    pub ports: Vec<u16>,
    /// Signing keys for each node
    pub keys: Vec<SigningKey>,
    /// Shared governance instance
    pub governance: Arc<G>,
}

impl<G, A> TestCluster<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Start all nodes in the cluster
    pub async fn start_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (i, node) in self.nodes.iter().enumerate() {
            println!("Starting test cluster node {} ({})", i, node.node_id());
            node.start().await?;
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

            for node in &self.nodes {
                if node.is_leader() {
                    leader_count += 1;
                }
                cluster_sizes.push(node.cluster_size());
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
        for (i, node) in self.nodes.iter().enumerate() {
            if let Err(e) = node.shutdown().await {
                println!("⚠️ Node {} shutdown error: {}", i, e);
            } else {
                println!("✅ Node {} shutdown successfully", i);
            }
        }
    }

    /// Get the current leader node, if any
    pub fn get_leader(&self) -> Option<&Consensus<G, A>> {
        self.nodes.iter().find(|node| node.is_leader())
    }

    /// Get a specific node by index
    pub fn get_node(&self, index: usize) -> Option<&Consensus<G, A>> {
        self.nodes.get(index)
    }

    /// Get the number of nodes in the cluster
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the cluster is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// Test single node with tcp transport
#[cfg(any(test, feature = "test-helpers"))]
pub async fn test_single_node_tcp(port: u16) -> Consensus<MockGovernance, MockAttestor> {
    let signing_key = SigningKey::generate(&mut OsRng);
    let governance = Arc::new(create_single_node_governance(port, &signing_key));
    let attestor = Arc::new(MockAttestor::new());

    let config = ConsensusConfigBuilder::new()
        .governance(governance)
        .attestor(attestor)
        .signing_key(signing_key)
        .transport_config(TransportConfig::Tcp {
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
        })
        .storage_config(StorageConfig::Memory)
        .build()
        .expect("Failed to build test consensus config");

    Consensus::new(config)
        .await
        .expect("Failed to create test consensus node")
}

/// Test single node with in-memory storage
#[cfg(any(test, feature = "test-helpers"))]
pub async fn test_single_node_memory() -> (Consensus<MockGovernance, MockAttestor>, u16) {
    let port = proven_util::port_allocator::allocate_port();
    let consensus = test_single_node_tcp(port).await;
    (consensus, port)
}

/// Test single node with rocksdb storage
#[cfg(any(test, feature = "test-helpers"))]
pub async fn test_single_node_rocksdb()
-> (Consensus<MockGovernance, MockAttestor>, tempfile::TempDir) {
    let port = proven_util::port_allocator::allocate_port();
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

    let signing_key = SigningKey::generate(&mut OsRng);
    let governance = Arc::new(create_single_node_governance(port, &signing_key));
    let attestor = Arc::new(MockAttestor::new());

    let config = ConsensusConfigBuilder::new()
        .governance(governance)
        .attestor(attestor)
        .signing_key(signing_key)
        .transport_config(TransportConfig::Tcp {
            listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
        })
        .storage_config(StorageConfig::RocksDB {
            path: temp_dir.path().to_path_buf(),
        })
        .build()
        .expect("Failed to build test consensus config");

    let consensus = Consensus::new(config)
        .await
        .expect("Failed to create test consensus node");
    (consensus, temp_dir)
}

/// Test single node with websocket transport
#[cfg(any(test, feature = "test-helpers"))]
pub async fn test_websocket_node() -> (
    Consensus<MockGovernance, MockAttestor>,
    u16,
    tokio::task::JoinHandle<()>,
) {
    let port = proven_util::port_allocator::allocate_port();

    let signing_key = SigningKey::generate(&mut OsRng);
    let governance = Arc::new(create_single_node_governance(port, &signing_key));
    let attestor = Arc::new(MockAttestor::new());

    let config = ConsensusConfigBuilder::new()
        .governance(governance)
        .attestor(attestor)
        .signing_key(signing_key)
        .transport_config(TransportConfig::WebSocket)
        .storage_config(StorageConfig::Memory)
        .build()
        .expect("Failed to build test consensus config");

    let consensus = Consensus::new(config)
        .await
        .expect("Failed to create test consensus node");

    // Start HTTP server for WebSocket transport
    let router = consensus.create_router().expect("Should create router");
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("Should bind to port");

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("HTTP server should run");
    });

    // Brief delay to let server start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (consensus, port, server_handle)
}

/// Test multi-node cluster with in-memory storage
#[cfg(any(test, feature = "test-helpers"))]
pub async fn test_multi_node_cluster(
    node_count: usize,
) -> TestCluster<MockGovernance, MockAttestor> {
    let mut ports = Vec::new();
    let mut keys = Vec::new();

    // Allocate ports and generate keys
    for _ in 0..node_count {
        ports.push(proven_util::port_allocator::allocate_port());
        keys.push(SigningKey::generate(&mut OsRng));
    }

    // Create shared governance
    let shared_governance = Arc::new(create_multi_node_governance(&ports, &keys));

    // Create consensus nodes
    let mut nodes = Vec::new();
    for (port, signing_key) in ports.iter().zip(keys.iter()) {
        let attestor = Arc::new(MockAttestor::new());

        let config = ConsensusConfigBuilder::new()
            .governance(shared_governance.clone())
            .attestor(attestor)
            .signing_key(signing_key.clone())
            .transport_config(TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            })
            .storage_config(StorageConfig::Memory)
            .build()
            .expect("Failed to build test consensus config");

        let consensus = Consensus::new(config)
            .await
            .expect("Failed to create test consensus node");
        nodes.push(consensus);
    }

    TestCluster {
        nodes,
        ports,
        keys,
        governance: shared_governance,
    }
}

/// Create single node governance
#[cfg(any(test, feature = "test-helpers"))]
pub fn create_single_node_governance(port: u16, signing_key: &SigningKey) -> MockGovernance {
    let attestor = MockAttestor::new();
    let actual_pcrs = attestor.pcrs_sync();

    let test_version = Version {
        ne_pcr0: actual_pcrs.pcr0,
        ne_pcr1: actual_pcrs.pcr1,
        ne_pcr2: actual_pcrs.pcr2,
    };

    let topology_node = GovernanceNode {
        availability_zone: "test-az".to_string(),
        origin: format!("127.0.0.1:{}", port),
        public_key: signing_key.verifying_key(),
        region: "test-region".to_string(),
        specializations: HashSet::new(),
    };

    MockGovernance::new(
        vec![topology_node],
        vec![test_version],
        "http://localhost:3200".to_string(),
        vec![],
    )
}

/// Create multi-node governance
#[cfg(any(test, feature = "test-helpers"))]
pub fn create_multi_node_governance(ports: &[u16], keys: &[SigningKey]) -> MockGovernance {
    let attestor = MockAttestor::new();
    let actual_pcrs = attestor.pcrs_sync();

    let test_version = Version {
        ne_pcr0: actual_pcrs.pcr0,
        ne_pcr1: actual_pcrs.pcr1,
        ne_pcr2: actual_pcrs.pcr2,
    };

    let governance = MockGovernance::new(
        vec![], // Start with empty topology
        vec![test_version],
        "http://localhost:3200".to_string(),
        vec![],
    );

    // Add all nodes to the shared governance
    for (port, signing_key) in ports.iter().zip(keys.iter()) {
        let node = GovernanceNode {
            availability_zone: "test-az".to_string(),
            origin: format!("http://127.0.0.1:{}", port),
            public_key: signing_key.verifying_key(),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        governance
            .add_node(node)
            .expect("Failed to add node to governance");
    }

    governance
}
