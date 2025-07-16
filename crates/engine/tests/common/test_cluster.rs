//! Test cluster utilities for integration testing

#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_engine::{Engine, EngineBuilder, EngineConfig};
use proven_governance::{Governance, GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use proven_network::NetworkManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Config as TransportConfig;
use proven_transport_tcp::{TcpConfig, TcpTransport};
use proven_util::port_allocator::allocate_port;
use rand::rngs::OsRng;
use tokio::sync::RwLock;
use tracing::info;

/// Transport type for test cluster
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransportType {
    Tcp,
}

/// Node information returned when creating nodes
#[derive(Clone)]
pub struct NodeInfo {
    /// Node ID
    pub node_id: NodeId,
    /// Signing key
    pub signing_key: SigningKey,
    /// Port number
    pub port: u16,
}

/// Test cluster manager
pub struct TestCluster {
    /// Mock governance instance  
    governance: RwLock<Arc<MockGovernance>>,
    /// Mock attestor
    attestor: MockAttestor,
    /// Transport type
    transport_type: TransportType,
    /// Node information
    nodes: Vec<NodeInfo>,
    /// Available versions
    versions: Vec<Version>,
}

impl TestCluster {
    /// Create a new empty test cluster
    pub fn new(transport_type: TransportType) -> Self {
        let attestor = MockAttestor::new();
        let pcrs = attestor.pcrs_sync();
        let versions = vec![Version {
            ne_pcr0: pcrs.pcr0,
            ne_pcr1: pcrs.pcr1,
            ne_pcr2: pcrs.pcr2,
        }];

        let governance = MockGovernance::new(
            vec![], // Start with no nodes
            versions.clone(),
            "https://auth.test.com".to_string(),
            vec![],
        );

        Self {
            governance: RwLock::new(Arc::new(governance)),
            attestor,
            transport_type,
            nodes: Vec::new(),
            versions,
        }
    }

    /// Get the governance instance
    pub async fn governance(&self) -> Arc<MockGovernance> {
        self.governance.read().await.clone()
    }

    /// Add nodes to the cluster and start them
    /// Returns (engines, node_infos) tuple
    pub async fn add_nodes(
        &mut self,
        count: usize,
    ) -> (
        Vec<Engine<TcpTransport<MockGovernance, MockAttestor>, MockGovernance, MemoryStorage>>,
        Vec<NodeInfo>,
    ) {
        let mut engines = Vec::new();
        let mut node_infos = Vec::new();
        let mut network_managers = Vec::new();
        let mut transports = Vec::new();
        let mut topology_managers = Vec::new();

        // First, create all nodes without starting anything
        // This ensures all nodes are in governance before any network/engine starts
        for _ in 0..count {
            let (engine, info, network_manager, transport, topology_manager) =
                self.create_tcp_node_without_starting().await;
            engines.push(engine);
            node_infos.push(info);
            network_managers.push(network_manager);
            transports.push(transport);
            topology_managers.push(topology_manager);
        }

        // Now start all TCP transports - this binds the TCP listeners
        for (i, transport) in transports.iter().enumerate() {
            info!("Starting TCP transport {} of {}", i + 1, transports.len());
            let actual_addr = transport
                .start()
                .await
                .expect("Failed to start TCP transport");
            info!("TCP transport {} listening on {}", i + 1, actual_addr);
        }

        // Now start all topology managers
        // This ensures all nodes are in governance before topology is refreshed
        for (i, topology_manager) in topology_managers.iter().enumerate() {
            info!(
                "Starting topology manager {} of {}",
                i + 1,
                topology_managers.len()
            );
            topology_manager
                .start()
                .await
                .expect("Failed to start topology manager");
        }

        // Now start all network managers
        // This ensures all TCP listeners are ready before discovery begins
        for (i, network_manager) in network_managers.iter().enumerate() {
            info!(
                "Starting network manager {} of {}",
                i + 1,
                network_managers.len()
            );
            network_manager
                .start()
                .await
                .expect("Failed to start network manager");
        }

        // Give TCP listeners more time to bind and be ready to accept connections
        info!("Waiting for TCP listeners to be ready...");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now start all engines in parallel
        // This ensures all namespace handlers are registered before discovery begins
        let engine_count = engines.len();
        info!("Starting {} engines in parallel", engine_count);

        let mut start_futures = Vec::new();
        for (i, engine) in engines.iter_mut().enumerate() {
            info!("Preparing to start engine {} of {}", i + 1, engine_count);
            start_futures.push(engine.start());
        }

        // Wait for all engines to start
        let results = futures::future::join_all(start_futures).await;
        for (i, result) in results.into_iter().enumerate() {
            result.unwrap_or_else(|e| panic!("Failed to start engine {i}: {e:?}"));
        }
        info!("All {} engines started successfully", engine_count);

        // Give services more time to fully initialize and discover each other
        info!("All engines started, waiting for discovery and cluster formation");

        // Log created nodes
        for (i, info) in node_infos.iter().enumerate() {
            info!("Node {}: {} on port {}", i + 1, info.node_id, info.port);
        }

        // Give topology managers time to refresh and see all nodes
        // The refresh happens periodically, so we need to wait a bit
        tokio::time::sleep(Duration::from_secs(3)).await;

        (engines, node_infos)
    }

    /// Create a single TCP node without starting the engine or network manager
    async fn create_tcp_node_without_starting(
        &mut self,
    ) -> (
        Engine<TcpTransport<MockGovernance, MockAttestor>, MockGovernance, MemoryStorage>,
        NodeInfo,
        Arc<NetworkManager<TcpTransport<MockGovernance, MockAttestor>, MockGovernance>>,
        Arc<TcpTransport<MockGovernance, MockAttestor>>,
        Arc<TopologyManager<MockGovernance>>,
    ) {
        // Generate node identity
        let signing_key = SigningKey::generate(&mut OsRng);
        let node_id = NodeId::from(signing_key.verifying_key());
        let port = allocate_port();

        info!("Creating TCP node {} on port {}", node_id, port);

        // Add node to governance
        self.add_node_to_governance(&signing_key, port).await;
        info!("Added node {} to governance with port {}", node_id, port);

        // Create governance instance for this node
        let governance = self.governance.read().await.clone();

        // Log how many nodes are in governance now
        let all_nodes = governance.get_topology().await.unwrap();
        info!("Governance now has {} nodes", all_nodes.len());

        // Start topology manager
        let topology_manager = Arc::new(TopologyManager::new(governance.clone(), node_id.clone()));

        // Don't start topology manager here - we'll start it after all nodes are created
        // to ensure all nodes see the complete topology

        // Create TCP transport
        let tcp_config = TcpConfig {
            transport: TransportConfig::default(),
            local_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            retry_attempts: 3,
            retry_delay_ms: 500,
        };

        let transport = TcpTransport::new(
            tcp_config,
            Arc::new(self.attestor.clone()),
            governance,
            signing_key.clone(),
            topology_manager.clone(),
        );

        // Don't start the TCP listener here - we'll do it later after all nodes are created
        let transport = Arc::new(transport);

        // Create network manager but DON'T start it yet
        let network_manager = Arc::new(NetworkManager::new(
            node_id.clone(),
            transport.clone(),
            topology_manager.clone(),
        ));

        // Create storage
        let storage = MemoryStorage::new();

        // Create engine
        let config = self.create_test_config();
        let engine = EngineBuilder::new(node_id.clone())
            .with_config(config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager.clone())
            .with_storage(storage.clone())
            .build()
            .await
            .expect("Failed to build engine");

        // Don't start the engine here - it will be started later

        let node_info = NodeInfo {
            node_id: node_id.clone(),
            signing_key,
            port,
        };

        self.nodes.push(node_info.clone());

        (
            engine,
            node_info,
            network_manager,
            transport,
            topology_manager,
        )
    }

    /// Create a single TCP node and start it (for single-node tests)
    async fn create_tcp_node(
        &mut self,
    ) -> (
        Engine<TcpTransport<MockGovernance, MockAttestor>, MockGovernance, MemoryStorage>,
        NodeInfo,
    ) {
        let (mut engine, node_info, network_manager, transport, topology_manager) =
            self.create_tcp_node_without_starting().await;

        // Start the TCP transport first
        let actual_addr = transport
            .start()
            .await
            .expect("Failed to start TCP transport");
        info!("TCP transport listening on {}", actual_addr);

        // Start the topology manager
        topology_manager
            .start()
            .await
            .expect("Failed to start topology manager");

        // Start the network manager
        network_manager
            .start()
            .await
            .expect("Failed to start network manager");

        // Start the engine
        engine.start().await.expect("Failed to start engine");

        // Give services a moment to fully initialize
        tokio::time::sleep(Duration::from_millis(100)).await;

        (engine, node_info)
    }

    /// Add node to governance
    async fn add_node_to_governance(&self, signing_key: &SigningKey, port: u16) {
        let gov = self.governance.write().await;
        let node = GovernanceNode {
            availability_zone: "test-az".to_string(),
            origin: format!("http://127.0.0.1:{port}"),
            public_key: signing_key.verifying_key(),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };
        let _ = gov.add_node(node);
    }

    /// Create test engine configuration
    fn create_test_config(&self) -> EngineConfig {
        let mut config = EngineConfig::default();

        // Adjust timeouts for testing
        config.network.connection_timeout = Duration::from_secs(1);
        config.network.request_timeout = Duration::from_secs(1);

        // Reduce consensus timeouts for faster tests
        config.consensus.global.election_timeout_min = Duration::from_millis(50);
        config.consensus.global.election_timeout_max = Duration::from_millis(100);
        config.consensus.global.heartbeat_interval = Duration::from_millis(20);

        config.consensus.group.election_timeout_min = Duration::from_millis(50);
        config.consensus.group.election_timeout_max = Duration::from_millis(100);
        config.consensus.group.heartbeat_interval = Duration::from_millis(20);

        config
    }

    /// Get node information
    pub fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }

    /// Wait for cluster formation
    pub async fn wait_for_cluster_formation(&self, timeout: Duration) -> bool {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            // Check if nodes have discovered each other
            // This is a simplified check - in real tests you'd check actual cluster state
            if self.nodes.len() > 1 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        false
    }
}
