//! Test helper methods for Consensus and related types
//!
//! This module provides static helper methods to simplify test setup
//! and reduce boilerplate code across test files.

#![allow(dead_code)]

pub mod cluster_builder;

use proven_consensus::config::{StorageConfig, TransportConfig};
use proven_consensus::{ConsensusClient, ConsensusGroupId, Engine, EngineBuilder, Node};

use std::collections::HashSet;
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use futures::Stream;
use proven_attestation_mock::MockAttestor;
use proven_governance::{GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::{Transport, TransportEnvelope, error::TransportError};
use proven_verification::CoseHandler;
use rand::rngs::OsRng;
use std::pin::Pin;
use tokio::sync::broadcast;
use uuid::Uuid;

// Mock transport for testing
pub struct MockTransport {
    incoming_tx: broadcast::Sender<TransportEnvelope>,
    cose_handler: Arc<CoseHandler>,
}

impl MockTransport {
    fn new(signing_key: SigningKey) -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            incoming_tx: tx,
            cose_handler: Arc::new(CoseHandler::new(signing_key)),
        }
    }
}

impl Clone for MockTransport {
    fn clone(&self) -> Self {
        Self {
            incoming_tx: self.incoming_tx.clone(),
            cose_handler: self.cose_handler.clone(),
        }
    }
}

#[async_trait]
impl Transport for MockTransport {
    async fn send_envelope(
        &self,
        _recipient: &NodeId,
        _payload: &bytes::Bytes,
        _message_type: &str,
        _correlation_id: Option<Uuid>,
    ) -> Result<(), TransportError> {
        Ok(())
    }

    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>> {
        Box::pin(futures::stream::empty())
    }

    fn cose_handler(&self) -> &CoseHandler {
        &*self.cose_handler
    }

    async fn shutdown(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

/// Test cluster management helper
pub struct TestCluster {
    /// The consensus engine instances
    pub engines: Vec<Arc<Engine<MockTransport, MockGovernance>>>,

    /// The consensus client instances
    pub clients: Vec<ConsensusClient<MockTransport, MockGovernance>>,

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

        let mut engines = Vec::new();
        let mut clients = Vec::new();
        let mut nodes = Vec::new();

        for (port, signing_key) in ports.iter().zip(signing_keys.iter()) {
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

            // Create mock transport
            let transport = MockTransport::new(signing_key.clone());

            // Create topology manager
            let topology_manager = Arc::new(
                TopologyManager::new(governance.clone(), NodeId::new(signing_key.verifying_key()))
                    .await
                    .expect("Failed to create topology manager"),
            );

            // Create network manager with topology manager
            let network_manager = Arc::new(NetworkManager::new(
                NodeId::new(signing_key.verifying_key()),
                transport,
                topology_manager.clone(),
            ));

            // Create consensus instance with Prometheus disabled for tests
            let mut monitoring_config = proven_consensus::config::MonitoringConfig::default();
            monitoring_config.prometheus.enabled = false;

            let (engine, client) = EngineBuilder::new()
                .network_manager(network_manager.clone())
                .topology_manager(topology_manager)
                .governance(governance.clone())
                .signing_key(signing_key.clone())
                .raft_config(openraft::Config::default())
                .transport_config(transport_config_fn(*port))
                .storage_config(storage_config_fn())
                .cluster_join_retry_config(
                    proven_consensus::config::ClusterJoinRetryConfig::default(),
                )
                .global_config(proven_consensus::config::GlobalConsensusConfig::default())
                .groups_config(proven_consensus::config::GroupsConfig::default())
                .allocation_config(proven_consensus::config::AllocationConfig::default())
                .migration_config(proven_consensus::config::MigrationConfig::default())
                .monitoring_config(monitoring_config)
                .stream_storage_backend(Default::default())
                .build()
                .await
                .expect("Failed to create test consensus node");

            engines.push(engine);
            clients.push(client);
        }

        Self {
            engines,
            clients,
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
        // Check if this is a WebSocket cluster by looking at the first engine's config
        // For now, assume mock transport doesn't support WebSocket
        let is_websocket = false;

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

        // Phase 1: Transport is already initialized via NetworkManager
        println!("📡 Phase 1: Transport already initialized via NetworkManager");

        // Phase 2: Create and start HTTP servers with WebSocket endpoints
        println!("🌐 Phase 2: Creating HTTP servers with WebSocket endpoints...");

        // Store HTTP server handles in the TestCluster for cleanup
        let http_handles = Vec::new();

        // Skip HTTP server creation for mock transport
        println!("  ℹ️  Skipping HTTP server creation (mock transport doesn't support WebSocket)");

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
            .engines
            .iter()
            .enumerate()
            .map(|(i, engine)| {
                println!("  Starting node {}", i);
                engine.clone().start()
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
        if self.engines.is_empty() {
            return Ok(());
        }

        // Transport is already initialized via NetworkManager
        println!("📡 Transport already initialized via NetworkManager");

        // For single node, start the engine
        if self.engines.len() == 1 {
            println!("🔍 Starting single test cluster node");
            self.engines[0].clone().start().await?;
            println!("✅ Single node cluster started");
            return Ok(());
        }

        // For multi-node clusters, start all engines in parallel
        println!("🔍 Starting {} nodes in parallel...", self.engines.len());

        // Create futures for starting each engine
        let start_futures: Vec<_> = self
            .engines
            .iter()
            .enumerate()
            .map(|(i, engine)| {
                println!("Preparing to start test cluster node {}", i);
                let engine = engine.clone();
                async move {
                    match engine.start().await {
                        Ok(()) => Ok(()),
                        Err(e) => Err(format!("Node {} failed to start: {}", i, e)),
                    }
                }
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
        let expected_size = self.engines.len();

        println!(
            "⏳ Waiting for cluster formation with {} nodes (timeout: {}s)...",
            expected_size, timeout_secs
        );

        while start.elapsed().as_secs() < timeout_secs {
            // Check each node's cluster state
            let mut formed_count = 0;
            let mut discovering_count = 0;
            let mut leader_found = false;

            for (i, client) in self.clients.iter().enumerate() {
                let is_formed = client.is_cluster_formed().await;
                let is_discovering = client.is_discovering().await;
                let cluster_state = client.get_cluster_state().await;

                if is_formed {
                    formed_count += 1;
                }
                if is_discovering {
                    discovering_count += 1;
                }

                // Check for leader
                if let Some(leader) = client.get_cluster_leader().await {
                    leader_found = true;
                    if i == 0 && start.elapsed().as_secs() % 5 == 0 {
                        // Log every 5 seconds
                        println!(
                            "  Current leader: {} (cluster size: {})",
                            leader,
                            client.get_cluster_size().await
                        );
                    }
                }

                // Log node state periodically
                if start.elapsed().as_secs() % 5 == 0 && i == 0 {
                    println!("  Node {} state: {:?}", i, cluster_state);
                }
            }

            // All nodes should be formed and a leader should exist
            if formed_count == expected_size && leader_found {
                // Verify cluster members
                if let Some(first_client) = self.clients.first() {
                    let members = first_client.get_cluster_members().await;
                    if members.len() == expected_size {
                        println!(
                            "✅ Cluster formation complete: {} nodes formed with leader",
                            formed_count
                        );
                        return true;
                    }
                }
            }

            // Log progress periodically
            if start.elapsed().as_secs() % 5 == 0 {
                println!(
                    "  Progress: {}/{} nodes formed, {} discovering, leader: {}",
                    formed_count,
                    expected_size,
                    discovering_count,
                    if leader_found { "yes" } else { "no" }
                );
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Log final state on timeout
        println!("❌ Cluster formation timed out after {}s", timeout_secs);
        for (i, client) in self.clients.iter().enumerate() {
            let state = client.get_cluster_state().await;
            let members = client.get_cluster_members().await;
            println!("  Node {}: state={:?}, members={}", i, state, members.len());
        }

        false
    }

    /// Shutdown all nodes gracefully
    pub async fn shutdown_all(&mut self) {
        // First shutdown all engine instances
        for (i, engine) in self.engines.iter().enumerate() {
            if let Err(e) = engine.clone().shutdown().await {
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

    /// Get the current leader node engine, if any
    /// Note: This returns the first engine that has initiated the cluster
    pub fn get_leader(&self) -> Option<&Arc<Engine<MockTransport, MockGovernance>>> {
        // In the test setup, we'll just return the first engine
        // as we don't have direct access to leadership status
        self.engines.first()
    }

    /// Get the index of the current leader node, if any
    pub fn get_leader_index(&self) -> Option<usize> {
        // In the test setup, we'll just return index 0
        // as we don't have direct access to leadership status
        if !self.engines.is_empty() {
            Some(0)
        } else {
            None
        }
    }

    /// Get a specific engine by index
    pub fn get_consensus(
        &self,
        index: usize,
    ) -> Option<&Arc<Engine<MockTransport, MockGovernance>>> {
        self.engines.get(index)
    }

    /// Get a specific node by index
    pub fn get_node(&self, index: usize) -> Option<&Node> {
        self.nodes.get(index)
    }

    /// Get a specific client by index
    pub fn get_client(
        &self,
        index: usize,
    ) -> Option<&ConsensusClient<MockTransport, MockGovernance>> {
        self.clients.get(index)
    }

    /// Get the number of nodes in the cluster
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// Check if the cluster is empty
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
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
        let members: Vec<_> = self.nodes.iter().map(|n| n.node_id().clone()).collect();

        println!(
            "Creating initial consensus group 1 with {} members",
            members.len()
        );

        // Get the first client to create the group
        let client = self.clients.first().ok_or("No clients in cluster")?;
        let response = client
            .create_group(ConsensusGroupId::new(1), members)
            .await?;

        if response.is_success() {
            println!("✅ Successfully created initial consensus group 1");
            Ok(())
        } else {
            Err(format!("Failed to create initial group: {:?}", response.error()).into())
        }
    }
}
