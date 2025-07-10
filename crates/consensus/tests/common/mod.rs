//! Test helper methods for Consensus and related types
//!
//! This module provides static helper methods to simplify test setup
//! and reduce boilerplate code across test files.

#![allow(dead_code)]

pub mod cluster_builder;

use proven_consensus::config::{
    ConsensusConfigBuilder, HierarchicalConsensusConfig, StorageConfig, TransportConfig,
};
use proven_consensus::{
    Consensus, Node, allocation::ConsensusGroupId, global::GlobalRequest,
    operations::GlobalOperation,
};

use std::collections::HashSet;
use std::{sync::Arc, time::Duration};

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_governance::{GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use rand::rngs::OsRng;

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

    /// Background task handles (e.g., HTTP servers for WebSocket)
    task_handles: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl TestCluster {
    /// Create a new test cluster builder for fluent configuration
    pub fn builder() -> cluster_builder::TestClusterBuilder {
        cluster_builder::TestClusterBuilder::new()
    }
    /// Private helper method to create a test cluster with specified configurations
    async fn new_internal(
        node_count: usize,
        transport_config_fn: impl Fn(u16) -> TransportConfig,
        storage_config_fn: impl Fn() -> StorageConfig,
    ) -> TestCluster {
        use proven_consensus::NodeId;

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
                proven_consensus::topology::TopologyManager::new(governance.clone(), node_id);
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
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
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
        // Check if this is a WebSocket cluster by looking at the first consensus instance's config
        let is_websocket = if let Some(first_consensus) = self.consensus_instances.first() {
            first_consensus.supports_http_integration()
        } else {
            return Ok(());
        };

        if is_websocket {
            // For WebSocket transport, use phased startup with HTTP servers
            self.start_all_websocket().await?;
        } else {
            // For non-WebSocket transports, use normal startup
            self.start_all_with_bootstrap().await?;
        }

        Ok(())
    }

    /// Start all WebSocket nodes with HTTP servers using phased startup
    async fn start_all_websocket(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Starting WebSocket cluster with HTTP servers...");

        // Phase 1: Initialize transport for all nodes (without starting discovery)
        println!("📡 Phase 1: Initializing WebSocket transports...");
        for (i, consensus) in self.consensus_instances.iter().enumerate() {
            consensus
                .initialize_transport()
                .await
                .map_err(|e| format!("Failed to initialize transport for node {}: {}", i, e))?;
            println!("  ✅ Node {} transport initialized", i);
        }

        // Phase 2: Create and start HTTP servers with WebSocket endpoints
        println!("🌐 Phase 2: Creating HTTP servers with WebSocket endpoints...");

        // Store HTTP server handles in the TestCluster for cleanup
        let mut http_handles = Vec::new();

        for (i, (consensus, &port)) in self
            .consensus_instances
            .iter()
            .zip(self.ports.iter())
            .enumerate()
        {
            // Get the WebSocket router from the consensus
            let router = consensus
                .create_router()
                .map_err(|e| format!("Failed to create router for node {}: {}", i, e))?;

            // Create the HTTP server
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
                .await
                .map_err(|e| {
                    format!(
                        "Failed to bind HTTP server for node {} on port {}: {}",
                        i, port, e
                    )
                })?;

            println!("  ✅ Node {} HTTP server listening on port {}", i, port);

            // Start the HTTP server in the background
            let server_handle = tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("HTTP server should run");
            });

            http_handles.push(server_handle);
        }

        // Store handles for cleanup
        {
            let mut task_handles = self.task_handles.lock().unwrap();
            task_handles.extend(http_handles);
        } // MutexGuard is dropped here

        println!("✅ All HTTP servers started with WebSocket endpoints");

        // Give HTTP servers time to start accepting connections
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Phase 3: Complete startup by triggering discovery for all nodes
        println!("🔍 Phase 3: Completing startup with discovery...");

        // Start discovery for all nodes concurrently
        let futures: Vec<_> = self
            .consensus_instances
            .iter()
            .enumerate()
            .map(|(i, consensus)| {
                println!("  Starting discovery for node {}", i);
                consensus.complete_startup()
            })
            .collect();

        // Wait for all nodes to complete startup
        let results = futures::future::join_all(futures).await;

        // Check results
        for (i, result) in results.into_iter().enumerate() {
            result.map_err(|e| format!("Failed to complete startup for node {}: {}", i, e))?;
            println!("  ✅ Node {} discovery completed", i);
        }

        println!("✅ All nodes completed startup with discovery");

        // Give nodes time to establish WebSocket connections
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(())
    }

    /// Start all nodes with proper bootstrap sequencing
    /// This ensures all nodes start in parallel and can discover each other
    pub async fn start_all_with_bootstrap(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.consensus_instances.is_empty() {
            return Ok(());
        }

        // First, initialize transport for all nodes
        println!("📡 Initializing transport for all nodes...");
        for (i, consensus) in self.consensus_instances.iter().enumerate() {
            consensus
                .initialize_transport()
                .await
                .map_err(|e| format!("Failed to initialize transport for node {}: {}", i, e))?;
            println!("  ✅ Node {} transport initialized", i);
        }

        // For single node, complete startup
        if self.consensus_instances.len() == 1 {
            println!(
                "🔍 Completing startup for single test cluster node ({})",
                self.consensus_instances[0].node_id()
            );
            self.consensus_instances[0].complete_startup().await?;
            println!("✅ Single node cluster started");
            return Ok(());
        }

        // For multi-node clusters, complete startup for all nodes in parallel
        println!(
            "🔍 Completing startup for {} nodes in parallel...",
            self.consensus_instances.len()
        );

        // Create futures for completing startup for each node
        let start_futures: Vec<_> = self
            .consensus_instances
            .iter()
            .enumerate()
            .map(|(i, consensus)| {
                let node_id = consensus.node_id().clone();
                println!(
                    "Preparing to complete startup for test cluster node {} ({})",
                    i, node_id
                );
                consensus.complete_startup()
            })
            .collect();

        // Complete startup for all nodes in parallel
        println!("Completing startup for all nodes concurrently...");
        let results = futures::future::join_all(start_futures).await;

        // Check if any node failed to start
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok(_) => println!("✅ Node {} completed startup successfully", i),
                Err(e) => {
                    println!("❌ Node {} failed to complete startup: {}", i, e);
                    return Err(format!("Node {} failed to complete startup: {}", i, e).into());
                }
            }
        }

        println!("✅ All {} test cluster nodes started", self.nodes.len());

        // Give nodes a bit of time to establish connections
        tokio::time::sleep(Duration::from_secs(1)).await;

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
        // First shutdown all consensus instances
        for (i, consensus) in self.consensus_instances.iter().enumerate() {
            if let Err(e) = consensus.shutdown().await {
                println!("⚠️ Node {} shutdown error: {}", i, e);
            } else {
                println!("✅ Node {} shutdown successfully", i);
            }
        }

        // Then abort any background tasks (e.g., HTTP servers)
        let mut handles = self.task_handles.lock().unwrap();
        for handle in handles.drain(..) {
            handle.abort();
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

    /// Ensure initial consensus group exists
    /// This creates consensus group 1 if it doesn't already exist
    pub async fn ensure_initial_group(&self) -> Result<(), Box<dyn std::error::Error>> {
        let leader = self.get_leader().ok_or("No leader found in cluster")?;

        // Check if group 1 already exists
        let global_state = leader.global_state();
        let existing_groups = global_state.get_all_groups().await;

        if existing_groups.iter().any(|g| g.id.0 == 1) {
            println!("✅ Initial consensus group 1 already exists");
            return Ok(());
        }

        // Create the initial group with all nodes as members
        let members: Vec<_> = self
            .consensus_instances
            .iter()
            .map(|c| c.node_id().clone())
            .collect();

        println!(
            "Creating initial consensus group 1 with {} members",
            members.len()
        );

        let request = GlobalRequest {
            operation: GlobalOperation::Group(
                proven_consensus::operations::GroupOperation::Create {
                    group_id: ConsensusGroupId::new(1),
                    initial_members: members,
                },
            ),
        };

        let response = leader.submit_request(request).await?;

        if response.success {
            println!("✅ Successfully created initial consensus group 1");
            Ok(())
        } else {
            Err(format!("Failed to create initial group: {:?}", response.error).into())
        }
    }
}

/// Create a test TopologyManager
/// Returns a topology manager with a pre-configured 3-node cluster
pub async fn create_test_topology_manager()
-> proven_consensus::topology::TopologyManager<MockGovernance> {
    let cluster = TestCluster::new_with_tcp_and_memory(3).await;
    let node_id = cluster.consensus_instances[0].node_id().clone();
    proven_consensus::topology::TopologyManager::new(cluster.governance, node_id)
}
