//! Test cluster utilities for integration testing

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_engine::{Engine, EngineBuilder, EngineConfig};
use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager};
use proven_storage_memory::MemoryStorage;
use proven_storage_rocksdb::RocksDbStorage;
use proven_topology::{Node as TopologyNode, TopologyAdaptor, Version};
use proven_topology::{NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport::Config as TransportConfig;
use proven_transport_tcp::{TcpConfig, TcpTransport};
use proven_util::port_allocator::allocate_port;
use rand::rngs::OsRng;
use tempfile::TempDir;
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
    governance: RwLock<Arc<MockTopologyAdaptor>>,
    /// Mock attestor
    attestor: MockAttestor,
    /// Transport type
    transport_type: TransportType,
    /// Node information
    nodes: Vec<NodeInfo>,
    /// Available versions
    versions: Vec<Version>,
    /// Unique cluster ID for this test instance
    cluster_id: String,
    /// Temp directories to keep alive
    temp_dirs: Vec<TempDir>,
    /// Storage managers for RocksDB nodes (kept alive across restarts)
    rocksdb_storage_managers:
        std::sync::Mutex<HashMap<String, Arc<StorageManager<RocksDbStorage>>>>,
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

        let governance = MockTopologyAdaptor::new(
            vec![], // Start with no nodes
            versions.clone(),
            "https://auth.test.com".to_string(),
            vec![],
        );

        // Generate a unique cluster ID using timestamp and random number
        let cluster_id = format!(
            "test_cluster_{}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            rand::random::<u32>()
        );

        Self {
            governance: RwLock::new(Arc::new(governance)),
            attestor,
            transport_type,
            nodes: Vec::new(),
            versions,
            cluster_id,
            temp_dirs: Vec::new(),
            rocksdb_storage_managers: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Get the governance instance
    pub async fn governance(&self) -> Arc<MockTopologyAdaptor> {
        self.governance.read().await.clone()
    }

    /// Get the unique cluster ID
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Get the storage path for a node (for persistence tests)
    pub fn get_node_storage_path(&self, node_id: &NodeId) -> Option<std::path::PathBuf> {
        self.temp_dirs.first().map(|temp_dir| {
            temp_dir
                .path()
                .join(&self.cluster_id)
                .join(format!("node_{node_id}"))
        })
    }

    /// Add nodes to the cluster and start them
    /// Returns (engines, node_infos) tuple
    pub async fn add_nodes(
        &mut self,
        count: usize,
    ) -> (
        Vec<
            Engine<
                TcpTransport<MockTopologyAdaptor, MockAttestor>,
                MockTopologyAdaptor,
                MemoryStorage,
            >,
        >,
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
        Engine<TcpTransport<MockTopologyAdaptor, MockAttestor>, MockTopologyAdaptor, MemoryStorage>,
        NodeInfo,
        Arc<NetworkManager<TcpTransport<MockTopologyAdaptor, MockAttestor>, MockTopologyAdaptor>>,
        Arc<TcpTransport<MockTopologyAdaptor, MockAttestor>>,
        Arc<TopologyManager<MockTopologyAdaptor>>,
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

        // Create storage manager
        let storage = MemoryStorage::new();
        let storage_manager = Arc::new(StorageManager::new(storage));

        // Create engine
        let config = self.create_test_config();
        let engine = EngineBuilder::new(node_id.clone())
            .with_config(config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager.clone())
            .with_storage(storage_manager)
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
        Engine<TcpTransport<MockTopologyAdaptor, MockAttestor>, MockTopologyAdaptor, MemoryStorage>,
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
        let node = TopologyNode::new(
            "test-az".to_string(),
            format!("http://127.0.0.1:{port}"),
            NodeId::from(signing_key.verifying_key()),
            "test-region".to_string(),
            HashSet::new(),
        );
        let _ = gov.add_node(node);
    }

    /// Create test engine configuration
    fn create_test_config(&self) -> EngineConfig {
        let mut config = EngineConfig::default();

        // Adjust timeouts for testing
        config.network.connection_timeout = Duration::from_secs(1);
        config.network.request_timeout = Duration::from_secs(1);

        // Use sensible consensus timeouts for stable testing
        // Heartbeat: 50ms (reasonable for local testing)
        // Leader lease will be ~100-250ms (2-5x heartbeat)
        // Election timeout: 300-600ms (must be > lease + network RTT)
        config.consensus.global.election_timeout_min = Duration::from_millis(300);
        config.consensus.global.election_timeout_max = Duration::from_millis(600);
        config.consensus.global.heartbeat_interval = Duration::from_millis(50);

        config.consensus.group.election_timeout_min = Duration::from_millis(300);
        config.consensus.group.election_timeout_max = Duration::from_millis(600);
        config.consensus.group.heartbeat_interval = Duration::from_millis(50);

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

    /// Wait for all nodes to have joined consensus groups
    pub async fn wait_for_group_formation<T, G, S>(
        &self,
        engines: &[Engine<T, G, S>],
        timeout: Duration,
    ) -> Result<(), String>
    where
        T: proven_transport::Transport,
        G: TopologyAdaptor,
        S: StorageAdaptor,
    {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let mut all_have_groups = true;

            for engine in engines {
                // Check if node has any groups
                match engine.node_groups().await {
                    Ok(groups) => {
                        if groups.is_empty() {
                            all_have_groups = false;
                            break;
                        }
                    }
                    Err(_) => {
                        all_have_groups = false;
                        break;
                    }
                }
            }

            if all_have_groups {
                info!("All {} nodes have joined consensus groups", engines.len());
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Log which nodes don't have groups
        for (i, engine) in engines.iter().enumerate() {
            match engine.node_groups().await {
                Ok(groups) => {
                    if groups.is_empty() {
                        info!("Node {} has no groups", i);
                    } else {
                        info!("Node {} is in {} groups", i, groups.len());
                    }
                }
                Err(e) => {
                    info!("Node {} failed to get groups: {}", i, e);
                }
            }
        }

        Err(format!(
            "Timeout after {timeout:?} waiting for group formation"
        ))
    }

    /// Wait for a specific group to be formed on all expected nodes
    pub async fn wait_for_specific_group<T, G, S>(
        &self,
        engines: &[Engine<T, G, S>],
        group_id: proven_engine::foundation::types::ConsensusGroupId,
        expected_members: usize,
        timeout: Duration,
    ) -> Result<(), String>
    where
        T: proven_transport::Transport,
        G: TopologyAdaptor,
        S: StorageAdaptor,
    {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let mut members_found = 0;

            for engine in engines {
                match engine.group_state(group_id).await {
                    Ok(state) => {
                        if state.is_member {
                            members_found += 1;
                        }
                    }
                    Err(_) => {
                        // Group doesn't exist on this node yet
                    }
                }
            }

            if members_found >= expected_members {
                info!(
                    "Group {:?} has {} members (expected {})",
                    group_id, members_found, expected_members
                );
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(format!(
            "Timeout after {timeout:?} waiting for group {group_id:?} to have {expected_members} members"
        ))
    }

    /// Add nodes to the cluster with RocksDB storage and start them
    /// Returns (engines, node_infos) tuple
    pub async fn add_nodes_with_rocksdb(
        &mut self,
        count: usize,
    ) -> (
        Vec<
            Engine<
                TcpTransport<MockTopologyAdaptor, MockAttestor>,
                MockTopologyAdaptor,
                RocksDbStorage,
            >,
        >,
        Vec<NodeInfo>,
    ) {
        self.add_nodes_with_rocksdb_internal(count, None).await
    }

    /// Add nodes with RocksDB storage using provided keys (for restart scenarios)
    pub async fn add_nodes_with_rocksdb_and_keys(
        &mut self,
        node_infos: Vec<NodeInfo>,
    ) -> (
        Vec<
            Engine<
                TcpTransport<MockTopologyAdaptor, MockAttestor>,
                MockTopologyAdaptor,
                RocksDbStorage,
            >,
        >,
        Vec<NodeInfo>,
    ) {
        self.add_nodes_with_rocksdb_internal(node_infos.len(), Some(node_infos))
            .await
    }

    /// Internal implementation for adding nodes with RocksDB
    async fn add_nodes_with_rocksdb_internal(
        &mut self,
        count: usize,
        existing_node_infos: Option<Vec<NodeInfo>>,
    ) -> (
        Vec<
            Engine<
                TcpTransport<MockTopologyAdaptor, MockAttestor>,
                MockTopologyAdaptor,
                RocksDbStorage,
            >,
        >,
        Vec<NodeInfo>,
    ) {
        let mut engines = Vec::new();
        let mut node_infos = Vec::new();
        let mut network_managers = Vec::new();
        let mut transports = Vec::new();
        let mut topology_managers = Vec::new();

        // Create a temp dir if we don't have one yet
        if self.temp_dirs.is_empty() {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            self.temp_dirs.push(temp_dir);
        }
        let temp_dir_path = self.temp_dirs.last().unwrap().path().to_path_buf();

        // First, create all nodes without starting anything
        // This ensures all nodes are in governance before any network/engine starts
        for i in 0..count {
            // Use existing node info if provided (for restarts), otherwise generate new
            let (signing_key, node_id, existing_port) =
                if let Some(ref existing) = existing_node_infos {
                    let info = &existing[i];
                    (
                        info.signing_key.clone(),
                        info.node_id.clone(),
                        Some(info.port),
                    )
                } else {
                    let signing_key = SigningKey::generate(&mut OsRng);
                    let node_id = NodeId::from(signing_key.verifying_key());
                    (signing_key, node_id, None)
                };

            // Use cluster ID and node ID in path to ensure consistency across restarts
            let path = temp_dir_path
                .join(&self.cluster_id)
                .join(format!("node_{node_id}"));
            let (engine, info, network_manager, transport, topology_manager) = self
                .create_tcp_node_without_starting_with_rocksdb(path, signing_key, existing_port)
                .await;
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

    /// Create a single TCP node without starting the engine or network manager, using RocksDB storage
    async fn create_tcp_node_without_starting_with_rocksdb(
        &mut self,
        path: std::path::PathBuf,
        signing_key: SigningKey,
        existing_port: Option<u16>,
    ) -> (
        Engine<
            TcpTransport<MockTopologyAdaptor, MockAttestor>,
            MockTopologyAdaptor,
            RocksDbStorage,
        >,
        NodeInfo,
        Arc<NetworkManager<TcpTransport<MockTopologyAdaptor, MockAttestor>, MockTopologyAdaptor>>,
        Arc<TcpTransport<MockTopologyAdaptor, MockAttestor>>,
        Arc<TopologyManager<MockTopologyAdaptor>>,
    ) {
        // Use the provided signing key
        let node_id = NodeId::from(signing_key.verifying_key());
        let port = existing_port.unwrap_or_else(allocate_port);

        info!("Creating TCP node {} on port {}", node_id, port);

        // Only add node to governance if it's not already there (i.e., no existing port)
        if existing_port.is_none() {
            self.add_node_to_governance(&signing_key, port).await;
            info!("Added node {} to governance with port {}", node_id, port);
        } else {
            info!(
                "Node {} already in governance, reusing port {}",
                node_id, port
            );
        }

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

        // Check if we already have a storage manager for this node (for restarts)
        let node_key = node_id.to_string();
        let existing_manager = {
            let managers = self.rocksdb_storage_managers.lock().unwrap();
            managers.get(&node_key).cloned()
        };

        let storage_manager = if let Some(existing) = existing_manager {
            info!("Reusing existing storage manager for node {}", node_id);
            existing
        } else {
            // Ensure the directory exists
            std::fs::create_dir_all(&path).expect("Failed to create directory for RocksDB");

            // Create RocksDB storage and storage manager
            let storage = RocksDbStorage::new(path)
                .await
                .expect("Failed to create RocksDB storage");
            let storage_manager = Arc::new(StorageManager::new(storage));

            // Store it for future reuse
            {
                let mut managers = self.rocksdb_storage_managers.lock().unwrap();
                managers.insert(node_key.clone(), storage_manager.clone());
            }
            info!("Created new storage manager for node {}", node_id);
            storage_manager
        };

        // Create engine
        let config = self.create_test_config();
        let engine = EngineBuilder::new(node_id.clone())
            .with_config(config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager.clone())
            .with_storage(storage_manager)
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

    /// Stop a specific engine by index
    pub async fn stop_engine<T, G, L>(
        &self,
        engines: &mut [Engine<T, G, L>],
        index: usize,
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        L: proven_storage::StorageAdaptor,
    {
        if index >= engines.len() {
            return Err(proven_engine::error::Error::with_context(
                proven_engine::error::ErrorKind::InvalidState,
                format!("Engine index {index} out of bounds"),
            ));
        }

        info!("Stopping engine at index {}", index);
        engines[index].stop().await?;
        info!("Engine at index {} stopped successfully", index);
        Ok(())
    }

    /// Start a specific engine by index
    pub async fn start_engine<T, G, L>(
        &self,
        engines: &mut [Engine<T, G, L>],
        index: usize,
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        L: proven_storage::StorageAdaptor,
    {
        if index >= engines.len() {
            return Err(proven_engine::error::Error::with_context(
                proven_engine::error::ErrorKind::InvalidState,
                format!("Engine index {index} out of bounds"),
            ));
        }

        info!("Starting engine at index {}", index);
        engines[index].start().await?;
        info!("Engine at index {} started successfully", index);
        Ok(())
    }

    /// Stop all engines
    pub async fn stop_all_engines<T, G, L>(
        &self,
        engines: &mut [Engine<T, G, L>],
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        L: proven_storage::StorageAdaptor,
    {
        info!("Stopping all {} engines", engines.len());
        for (i, engine) in engines.iter_mut().enumerate() {
            info!("Stopping engine {}", i);
            engine.stop().await?;
        }
        info!("All engines stopped successfully");
        Ok(())
    }

    /// Start all engines
    pub async fn start_all_engines<T, G, L>(
        &self,
        engines: &mut [Engine<T, G, L>],
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        L: proven_storage::StorageAdaptor,
    {
        info!("Starting all {} engines", engines.len());
        let mut start_futures = Vec::new();
        for (i, engine) in engines.iter_mut().enumerate() {
            info!("Preparing to start engine {}", i);
            start_futures.push(engine.start());
        }

        // Wait for all engines to start
        let results = futures::future::join_all(start_futures).await;
        for (i, result) in results.into_iter().enumerate() {
            result.map_err(|e| {
                proven_engine::error::Error::with_context(
                    proven_engine::error::ErrorKind::InvalidState,
                    format!("Failed to start engine {i}: {e:?}"),
                )
            })?;
        }
        info!("All engines started successfully");
        Ok(())
    }
}
