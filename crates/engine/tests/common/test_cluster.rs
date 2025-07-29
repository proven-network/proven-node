//! Test cluster utilities for integration testing

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use futures::future;
use proven_attestation_mock::MockAttestor;
use proven_engine::{Engine, EngineBuilder, EngineConfig};
use proven_network::{NetworkManager, connection_pool::ConnectionPoolConfig};
use proven_storage::{StorageAdaptor, StorageManager};
use proven_storage_memory::MemoryStorage;
use proven_storage_rocksdb::RocksDbStorage;
use proven_topology::{Node as TopologyNode, TopologyAdaptor, Version};
use proven_topology::{NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_tcp::{TcpOptions, TcpTransport};
use proven_util::port_allocator::allocate_port;
use rand::rngs::OsRng;
use tempfile::TempDir;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

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
    rocksdb_storage_managers: Mutex<HashMap<String, Arc<StorageManager<RocksDbStorage>>>>,
    /// Topology managers for test access
    topology_managers: Mutex<Vec<Arc<TopologyManager<MockTopologyAdaptor>>>>,
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
            rocksdb_storage_managers: Mutex::new(HashMap::new()),
            topology_managers: Mutex::new(Vec::new()),
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
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>>,
        Vec<NodeInfo>,
    ) {
        self.add_nodes_with_mode(count, true).await
    }

    /// Add nodes to the cluster without starting them
    /// Returns (engines, node_infos) tuple
    pub async fn add_nodes_without_starting(
        &mut self,
        count: usize,
    ) -> (
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>>,
        Vec<NodeInfo>,
    ) {
        self.add_nodes_with_mode(count, false).await
    }

    /// Internal method to add nodes with optional auto-start
    async fn add_nodes_with_mode(
        &mut self,
        count: usize,
        auto_start: bool,
    ) -> (
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>>,
        Vec<NodeInfo>,
    ) {
        let mut engines = Vec::new();
        let mut node_infos = Vec::new();

        // Create and optionally start each node one at a time
        // This ensures nodes are added to topology incrementally
        for i in 0..count {
            if auto_start {
                info!("Creating and starting node {} of {}", i + 1, count);
            } else {
                info!("Creating node {} of {} (without starting)", i + 1, count);
            }

            let (engine, node_info) = if auto_start {
                self.create_tcp_node().await
            } else {
                self.create_tcp_node_without_starting().await
            };

            // Get short node ID for logging
            let node_id_str = node_info.node_id.to_string();
            let short_id = &node_id_str[..8];

            info!(
                "[Node-{}] Started: {} on port {}",
                short_id, node_info.node_id, node_info.port
            );

            engines.push(engine);
            node_infos.push(node_info);

            // After adding each node, refresh topology on ALL nodes so they see each other
            let manager_count = self.topology_managers.lock().await.len();
            if manager_count > 1 {
                info!(
                    "Refreshing topology on all {} nodes after adding node {}",
                    manager_count,
                    i + 1
                );

                // Give the first node's services time to fully initialize before the second node tries to connect
                info!("Waiting for services to initialize...");
                tokio::time::sleep(Duration::from_secs(1)).await;

                // Refresh all topology managers including the new node's
                let managers = self.topology_managers.lock().await;
                let refresh_futures: Vec<_> = managers
                    .iter()
                    .enumerate()
                    .map(|(idx, manager)| {
                        let manager = manager.clone();
                        async move {
                            if let Err(e) = manager.refresh_topology().await {
                                warn!("Failed to refresh topology on manager {}: {}", idx, e);
                            } else {
                                let peers = manager.get_all_peers().await;
                                info!("Manager {} now sees {} peers", idx, peers.len());
                            }
                        }
                    })
                    .collect();
                drop(managers); // Drop the lock before awaiting
                future::join_all(refresh_futures).await;

                // Wait for topology to propagate
                info!("Waiting for topology to propagate...");
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Verify all nodes can see each other
                let managers = self.topology_managers.lock().await;
                let peer_check_futures: Vec<_> = managers
                    .iter()
                    .enumerate()
                    .map(|(idx, manager)| {
                        let manager = manager.clone();
                        async move {
                            let peers = manager.get_all_peers().await;
                            info!(
                                "After propagation, manager {} sees {} peers",
                                idx,
                                peers.len()
                            );
                        }
                    })
                    .collect();
                drop(managers); // Drop the lock before awaiting
                future::join_all(peer_check_futures).await;
            }
        }

        info!("All {} nodes created and started successfully", count);

        (engines, node_infos)
    }

    /// Create a single TCP node without starting it
    async fn create_tcp_node_without_starting(
        &mut self,
    ) -> (
        Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>,
        NodeInfo,
    ) {
        self.create_tcp_node_internal(false).await
    }

    /// Create a single TCP node and start it
    async fn create_tcp_node(
        &mut self,
    ) -> (
        Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>,
        NodeInfo,
    ) {
        self.create_tcp_node_internal(true).await
    }

    /// Internal method to create a TCP node with optional start
    async fn create_tcp_node_internal(
        &mut self,
        _start_immediately: bool,
    ) -> (
        Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, MemoryStorage>,
        NodeInfo,
    ) {
        // Generate node identity
        let signing_key = SigningKey::generate(&mut OsRng);
        let node_id = NodeId::from(signing_key.verifying_key());
        let port = allocate_port();

        // Get short node ID for logging
        let node_id_str = node_id.to_string();
        let short_id = &node_id_str[..8];

        info!(
            "[Node-{}] Creating TCP node {} on port {}",
            short_id, node_id, port
        );

        // Add node to governance
        self.add_node_to_governance(&signing_key, port).await;
        info!("[Node-{}] Added to governance with port {}", short_id, port);

        // Give governance a moment to propagate the update
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create governance instance for this node
        let governance = self.governance.read().await.clone();

        // Log how many nodes are in governance now
        let all_nodes = governance.get_topology().await.unwrap();
        info!(
            "[Node-{}] Governance now has {} nodes",
            short_id,
            all_nodes.len()
        );

        // Create topology manager with shorter refresh interval for tests
        let topology_config = proven_topology::TopologyManagerConfig {
            refresh_interval: std::time::Duration::from_secs(2), // 2 seconds for tests
        };
        let topology_manager = Arc::new(TopologyManager::with_config(
            governance.clone(),
            node_id.clone(),
            topology_config,
        ));

        // Store the topology manager for test access
        {
            let mut managers = self.topology_managers.lock().await;
            managers.push(topology_manager.clone());
        }

        // Create TCP transport
        let tcp_options = TcpOptions {
            listen_addr: Some(format!("127.0.0.1:{port}").parse().unwrap()),
        };

        let transport = TcpTransport::new(tcp_options);
        let transport = Arc::new(transport);

        // Create network manager
        let connection_pool_config = ConnectionPoolConfig::default();
        let network_manager = Arc::new(NetworkManager::new(
            node_id.clone(),
            transport.clone(),
            topology_manager.clone(),
            signing_key.clone(),
            connection_pool_config,
            governance.clone(),
            Arc::new(self.attestor.clone()),
        ));

        // Create storage manager
        let storage = MemoryStorage::new();
        let storage_manager = Arc::new(StorageManager::new(storage));

        // Create engine
        let config = self.create_test_config();
        let mut engine = EngineBuilder::new(node_id.clone())
            .with_config(config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager.clone())
            .with_storage(storage_manager)
            .build()
            .await
            .expect("Failed to build engine");

        // Start all components
        info!("[Node-{}] Starting components on port {}", short_id, port);

        topology_manager
            .start()
            .await
            .expect("Failed to start topology manager");

        // Force an immediate topology refresh so the node sees any existing nodes
        info!(
            "[Node-{}] Refreshing topology before starting network",
            short_id
        );
        if let Err(e) = topology_manager.refresh_topology().await {
            warn!("[Node-{}] Failed to refresh topology: {}", short_id, e);
        } else {
            let peers = topology_manager.get_all_peers().await;
            info!(
                "[Node-{}] Sees {} peers before engine start",
                short_id,
                peers.len()
            );
        }

        network_manager
            .start()
            .await
            .expect("Failed to start network manager");

        engine.start().await.expect("Failed to start engine");

        let node_info = NodeInfo {
            node_id: node_id.clone(),
            signing_key,
            port,
        };

        self.nodes.push(node_info.clone());

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

    /// Create a single TCP node with RocksDB and start it
    async fn create_tcp_node_with_rocksdb(
        &mut self,
        path: std::path::PathBuf,
        signing_key: Option<SigningKey>,
        existing_port: Option<u16>,
    ) -> (
        Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, RocksDbStorage>,
        NodeInfo,
    ) {
        // Use provided signing key or generate new one
        let signing_key = signing_key.unwrap_or_else(|| SigningKey::generate(&mut OsRng));
        let node_id = NodeId::from(signing_key.verifying_key());
        let port = existing_port.unwrap_or_else(allocate_port);

        // Get short node ID for logging
        let node_id_str = node_id.to_string();
        let short_id = &node_id_str[..8];

        info!(
            "[Node-{}] Creating TCP node {} on port {}",
            short_id, node_id, port
        );

        // Only add node to governance if it's not already there (i.e., no existing port)
        if existing_port.is_none() {
            self.add_node_to_governance(&signing_key, port).await;
            info!("[Node-{}] Added to governance with port {}", short_id, port);

            // Give governance a moment to propagate the update
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            info!(
                "[Node-{}] Already in governance, reusing port {}",
                short_id, port
            );
        }

        // Create governance instance for this node
        let governance = self.governance.read().await.clone();

        // Log how many nodes are in governance now
        let all_nodes = governance.get_topology().await.unwrap();
        info!(
            "[Node-{}] Governance now has {} nodes",
            short_id,
            all_nodes.len()
        );

        // Create topology manager with shorter refresh interval for tests
        let topology_config = proven_topology::TopologyManagerConfig {
            refresh_interval: std::time::Duration::from_secs(2), // 2 seconds for tests
        };
        let topology_manager = Arc::new(TopologyManager::with_config(
            governance.clone(),
            node_id.clone(),
            topology_config,
        ));

        // Store the topology manager for test access
        {
            let mut managers = self.topology_managers.lock().await;
            managers.push(topology_manager.clone());
        }

        // Create TCP transport
        let tcp_options = TcpOptions {
            listen_addr: Some(format!("127.0.0.1:{port}").parse().unwrap()),
        };

        let transport = TcpTransport::new(tcp_options);
        let transport = Arc::new(transport);

        // Create network manager
        let connection_pool_config = ConnectionPoolConfig::default();
        let network_manager = Arc::new(NetworkManager::new(
            node_id.clone(),
            transport.clone(),
            topology_manager.clone(),
            signing_key.clone(),
            connection_pool_config,
            governance.clone(),
            Arc::new(self.attestor.clone()),
        ));

        // Check if we already have a storage manager for this node (for restarts)
        let node_key = node_id.to_string();
        let existing_manager = {
            let managers = self.rocksdb_storage_managers.lock().await;
            managers.get(&node_key).cloned()
        };

        let storage_manager = if let Some(existing) = existing_manager {
            info!("[Node-{}] Reusing existing storage manager", short_id);
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
                let mut managers = self.rocksdb_storage_managers.lock().await;
                managers.insert(node_key.clone(), storage_manager.clone());
            }
            info!("[Node-{}] Created new storage manager", short_id);
            storage_manager
        };

        // Create engine
        let config = self.create_test_config();
        let mut engine = EngineBuilder::new(node_id.clone())
            .with_config(config)
            .with_network(network_manager.clone())
            .with_topology(topology_manager.clone())
            .with_storage(storage_manager)
            .build()
            .await
            .expect("Failed to build engine");

        // Start all components
        info!("[Node-{}] Starting components on port {}", short_id, port);

        topology_manager
            .start()
            .await
            .expect("Failed to start topology manager");

        // Force an immediate topology refresh so the node sees any existing nodes
        info!(
            "[Node-{}] Refreshing topology before starting network",
            short_id
        );
        if let Err(e) = topology_manager.refresh_topology().await {
            warn!("[Node-{}] Failed to refresh topology: {}", short_id, e);
        } else {
            let peers = topology_manager.get_all_peers().await;
            info!(
                "[Node-{}] Sees {} peers before engine start",
                short_id,
                peers.len()
            );
        }

        network_manager
            .start()
            .await
            .expect("Failed to start network manager");

        engine.start().await.expect("Failed to start engine");

        // Give services a moment to fully initialize
        tokio::time::sleep(Duration::from_millis(100)).await;

        let node_info = NodeInfo {
            node_id: node_id.clone(),
            signing_key,
            port,
        };

        self.nodes.push(node_info.clone());

        (engine, node_info)
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

    /// Wait for global cluster formation
    /// This waits for all nodes to discover each other and establish global consensus
    pub async fn wait_for_global_cluster<T, G, A, S>(
        &self,
        engines: &[Engine<T, G, A, S>],
        timeout: Duration,
    ) -> Result<(), String>
    where
        T: proven_transport::Transport,
        G: TopologyAdaptor,
        A: proven_attestation::Attestor,
        S: StorageAdaptor,
    {
        let start = std::time::Instant::now();

        // For now, we'll use a simple time-based wait to ensure nodes have time to:
        // 1. Discover each other via membership service
        // 2. Establish global consensus
        // 3. Create the default group

        // This is a pragmatic approach until we have a proper way to check
        // global consensus state directly
        let wait_time = std::cmp::min(Duration::from_secs(3), timeout);
        tokio::time::sleep(wait_time).await;

        // After the initial wait, verify that at least the coordinator has created groups
        let mut any_has_groups = false;
        for engine in engines {
            if let Ok(groups) = engine.client().node_groups().await
                && !groups.is_empty()
            {
                any_has_groups = true;
                info!(
                    "Found node with {} groups after global cluster wait",
                    groups.len()
                );
                break;
            }
        }

        if any_has_groups || start.elapsed() < timeout {
            info!(
                "Global cluster formation complete after {:?}",
                start.elapsed()
            );
            Ok(())
        } else {
            Err(format!(
                "Timeout after {timeout:?} waiting for global cluster formation"
            ))
        }
    }

    /// Wait for cluster formation (deprecated - use wait_for_global_cluster)
    #[deprecated(note = "Use wait_for_global_cluster instead")]
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

    /// Wait for the default group (ID 1) to become routable
    /// This only requires at least 1 member (typically the coordinator)
    pub async fn wait_for_default_group_routable<T, G, A, S>(
        &self,
        engines: &[Engine<T, G, A, S>],
        timeout: Duration,
    ) -> Result<(), String>
    where
        T: proven_transport::Transport,
        G: TopologyAdaptor,
        A: proven_attestation::Attestor,
        S: StorageAdaptor,
    {
        self.wait_for_specific_group(
            engines,
            proven_engine::foundation::types::ConsensusGroupId::new(1),
            1, // Only need 1 member for routability
            timeout,
        )
        .await
    }

    /// Wait for a specific group to be formed on all expected nodes
    pub async fn wait_for_specific_group<T, G, A, S>(
        &self,
        engines: &[Engine<T, G, A, S>],
        group_id: proven_engine::foundation::types::ConsensusGroupId,
        expected_members: usize,
        timeout: Duration,
    ) -> Result<(), String>
    where
        T: proven_transport::Transport,
        G: TopologyAdaptor,
        A: proven_attestation::Attestor,
        S: StorageAdaptor,
    {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let mut members_found = 0;

            for engine in engines {
                match engine.client().group_state(group_id).await {
                    Ok(Some(state)) => {
                        if state.is_member {
                            members_found += 1;
                        }
                    }
                    Ok(None) => {
                        // Group doesn't exist on this node yet
                    }
                    Err(_) => {
                        // Error getting group state
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
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, RocksDbStorage>>,
        Vec<NodeInfo>,
    ) {
        self.add_nodes_with_rocksdb_internal(count, None).await
    }

    /// Add nodes with RocksDB storage using provided keys (for restart scenarios)
    pub async fn add_nodes_with_rocksdb_and_keys(
        &mut self,
        node_infos: Vec<NodeInfo>,
    ) -> (
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, RocksDbStorage>>,
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
        Vec<Engine<TcpTransport, MockTopologyAdaptor, MockAttestor, RocksDbStorage>>,
        Vec<NodeInfo>,
    ) {
        let mut engines = Vec::new();
        let mut node_infos = Vec::new();

        // Create a temp dir if we don't have one yet
        if self.temp_dirs.is_empty() {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            self.temp_dirs.push(temp_dir);
        }
        let temp_dir_path = self.temp_dirs.last().unwrap().path().to_path_buf();

        // Create and fully start each node one at a time
        // This ensures nodes are added to topology incrementally
        for i in 0..count {
            info!("Creating and starting RocksDB node {} of {}", i + 1, count);

            // Use existing node info if provided (for restarts), otherwise generate new
            let (signing_key, node_id, existing_port) =
                if let Some(ref existing) = existing_node_infos {
                    let info = &existing[i];
                    (
                        Some(info.signing_key.clone()),
                        info.node_id.clone(),
                        Some(info.port),
                    )
                } else {
                    let key = SigningKey::generate(&mut OsRng);
                    let id = NodeId::from(key.verifying_key());
                    (Some(key), id, None)
                };

            // Use cluster ID and node ID in path to ensure consistency across restarts
            let path = temp_dir_path
                .join(&self.cluster_id)
                .join(format!("node_{node_id}"));

            let (engine, node_info) = self
                .create_tcp_node_with_rocksdb(path, signing_key, existing_port)
                .await;

            // Get short node ID for logging
            let node_id_str = node_info.node_id.to_string();
            let short_id = &node_id_str[..8];

            info!(
                "[Node-{}] RocksDB node started: {} on port {}",
                short_id, node_info.node_id, node_info.port
            );

            engines.push(engine);
            node_infos.push(node_info);

            // Force topology refresh on all nodes (including the new one) so they see each other
            if i > 0 {
                let manager_count = self.topology_managers.lock().await.len();
                info!("Refreshing topology on all {} nodes", manager_count);

                // Refresh all topology managers including the new node's
                let managers = self.topology_managers.lock().await;
                let refresh_futures: Vec<_> = managers
                    .iter()
                    .map(|manager| {
                        let manager = manager.clone();
                        async move {
                            if let Err(e) = manager.refresh_topology().await {
                                warn!("Failed to refresh topology: {}", e);
                            }
                        }
                    })
                    .collect();
                drop(managers); // Drop the lock before awaiting
                future::join_all(refresh_futures).await;

                // Wait for topology to propagate
                info!("Waiting for topology to propagate...");
                tokio::time::sleep(Duration::from_secs(1)).await;

                // Verify the new node can see existing nodes
                let managers = self.topology_managers.lock().await;
                if let Some(new_manager) = managers.last() {
                    let new_manager = new_manager.clone();
                    drop(managers); // Drop the lock before awaiting
                    let peers = new_manager.get_all_peers().await;
                    info!("New node sees {} peers after topology refresh", peers.len());
                    if peers.is_empty() {
                        warn!("WARNING: New node still sees no peers after topology refresh!");
                    }
                }
            }
        }

        info!(
            "All {} RocksDB nodes created and started successfully",
            count
        );

        (engines, node_infos)
    }

    /// Stop a specific engine by index
    pub async fn stop_engine<T, G, A, L>(
        &self,
        engines: &mut [Engine<T, G, A, L>],
        index: usize,
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        A: proven_attestation::Attestor,
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
    pub async fn start_engine<T, G, A, L>(
        &self,
        engines: &mut [Engine<T, G, A, L>],
        index: usize,
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        A: proven_attestation::Attestor,
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
    pub async fn stop_all_engines<T, G, A, L>(
        &self,
        engines: &mut [Engine<T, G, A, L>],
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        A: proven_attestation::Attestor,
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
    pub async fn start_all_engines<T, G, A, L>(
        &self,
        engines: &mut [Engine<T, G, A, L>],
    ) -> Result<(), proven_engine::error::Error>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        A: proven_attestation::Attestor,
        L: proven_storage::StorageAdaptor,
    {
        info!("Starting all {} engines", engines.len());
        let mut start_futures = Vec::new();
        for (i, engine) in engines.iter_mut().enumerate() {
            info!("Preparing to start engine {}", i);
            start_futures.push(engine.start());
        }

        // Wait for all engines to start
        let results = future::join_all(start_futures).await;
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

    /// Wait for all nodes to see the expected topology size
    pub async fn wait_for_topology_size<T, G, A, L>(
        &self,
        _engines: &[Engine<T, G, A, L>],
        expected_size: usize,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        T: proven_transport::Transport,
        G: proven_topology::TopologyAdaptor,
        A: proven_attestation::Attestor,
        L: proven_storage::StorageAdaptor,
    {
        let start = tokio::time::Instant::now();

        loop {
            // Force refresh on all topology managers
            {
                let managers = self.topology_managers.lock().await;
                let refresh_futures: Vec<_> = managers
                    .iter()
                    .map(|manager| {
                        let manager = manager.clone();
                        async move {
                            let _ = manager.force_refresh().await;
                        }
                    })
                    .collect();
                drop(managers); // Drop the lock before awaiting
                future::join_all(refresh_futures).await;
            }

            // Check if all nodes see the expected size
            let mut all_see_size = true;
            {
                let managers = self.topology_managers.lock().await;
                let node_check_futures: Vec<_> = managers
                    .iter()
                    .enumerate()
                    .map(|(i, manager)| {
                        let manager = manager.clone();
                        async move {
                            let nodes = manager.get_cached_nodes().await;
                            (i, nodes)
                        }
                    })
                    .collect();
                drop(managers); // Drop the lock before awaiting

                let results = future::join_all(node_check_futures).await;
                for (i, nodes) in results {
                    let nodes = nodes?;
                    if nodes.len() != expected_size {
                        all_see_size = false;
                        tracing::debug!(
                            "Node {} sees {} nodes, expected {}",
                            i,
                            nodes.len(),
                            expected_size
                        );
                        break;
                    }
                }
            }

            if all_see_size {
                tracing::info!("All nodes see {} nodes in topology", expected_size);
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err("Timeout waiting for topology size".into());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
