//! Test cluster builder for fluent test configuration
//!
//! This module provides a builder pattern for creating test clusters
//! with various configurations, making tests more readable and maintainable.

use proven_consensus::ConsensusGroupId;
use proven_consensus::config::{
    AllocationConfig, GlobalConsensusConfig, GroupsConfig, MigrationConfig, MonitoringConfig,
    StorageConfig, StreamConfig, TransportConfig,
};

use proven_consensus::{EngineBuilder, Node, NodeId};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use proven_attestation_mock::MockAttestor;
use proven_governance::{GovernanceNode, Version};
use proven_governance_mock::MockGovernance;
use proven_network::NetworkManager;
use proven_topology::TopologyManager;
use rand::rngs::OsRng;
use tempfile::TempDir;

/// Builder for creating test clusters with fluent configuration
pub struct TestClusterBuilder {
    /// Number of nodes in the cluster
    node_count: usize,
    /// Transport configuration to use
    transport: TransportType,
    /// Storage configuration to use
    storage: StorageType,
    /// Number of default consensus groups to create
    default_groups: usize,
    /// Streams to pre-create
    streams: Vec<StreamDefinition>,
    /// Custom hierarchical configuration
    // Individual config options
    global_config: Option<GlobalConsensusConfig>,
    groups_config: Option<GroupsConfig>,
    allocation_config: Option<AllocationConfig>,
    migration_config: Option<MigrationConfig>,
    /// Whether to start nodes automatically
    auto_start: bool,
    /// Whether to wait for cluster formation
    wait_for_formation: bool,
    /// Timeout for cluster formation
    formation_timeout: Duration,
    /// Custom node configurations
    node_configs: HashMap<usize, NodeConfig>,
    /// Whether to enable monitoring
    enable_monitoring: bool,
}

/// Transport type for test clusters
#[derive(Clone)]
pub enum TransportType {
    /// TCP transport with auto-allocated ports
    Tcp,
    /// WebSocket transport
    WebSocket,
}

/// Storage type for test clusters
#[derive(Clone)]
pub enum StorageType {
    /// In-memory storage
    Memory,
    /// RocksDB storage with temporary directories
    RocksDB,
}

/// Stream definition for pre-creating streams
#[derive(Clone)]
pub struct StreamDefinition {
    /// Stream name
    pub name: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Group to assign to (None = auto-select)
    pub group_id: Option<ConsensusGroupId>,
    /// Subject patterns to subscribe to
    pub subscriptions: Vec<String>,
}

/// Custom configuration for specific nodes
#[derive(Clone, Default)]
pub struct NodeConfig {
    /// Custom availability zone
    pub availability_zone: Option<String>,
    /// Custom region
    pub region: Option<String>,
    /// Node specializations
    pub specializations: HashSet<proven_governance::NodeSpecialization>,
    /// Whether this node should be offline initially
    pub start_offline: bool,
}

/// Result of building a test cluster
pub struct TestClusterResult {
    /// The test cluster instance
    pub cluster: super::TestCluster,
    /// Temporary directories (for RocksDB storage)
    pub temp_dirs: Vec<TempDir>,
    /// Stream definitions that were created
    pub streams: Vec<StreamDefinition>,
    /// Group IDs that were created
    pub group_ids: Vec<ConsensusGroupId>,
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        Self {
            node_count: 1,
            transport: TransportType::Tcp,
            storage: StorageType::Memory,
            default_groups: 1,
            streams: Vec::new(),
            global_config: None,
            groups_config: None,
            allocation_config: None,
            migration_config: None,
            auto_start: true,
            wait_for_formation: true,
            formation_timeout: Duration::from_secs(10),
            node_configs: HashMap::new(),
            enable_monitoring: false,
        }
    }
}

impl TestClusterBuilder {
    /// Create a new test cluster builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of nodes
    pub fn with_nodes(mut self, count: usize) -> Self {
        self.node_count = count;
        self
    }

    /// Set the transport type
    pub fn with_transport(mut self, transport: TransportType) -> Self {
        self.transport = transport;
        self
    }

    /// Set the storage type
    pub fn with_storage(mut self, storage: StorageType) -> Self {
        self.storage = storage;
        self
    }

    /// Set the number of default consensus groups
    pub fn with_default_groups(mut self, count: usize) -> Self {
        self.default_groups = count;
        self
    }

    /// Add streams to pre-create
    pub fn with_streams(mut self, streams: Vec<&str>) -> Self {
        self.streams = streams
            .into_iter()
            .map(|name| StreamDefinition {
                name: name.to_string(),
                config: StreamConfig::default(),
                group_id: None,
                subscriptions: Vec::new(),
            })
            .collect();
        self
    }

    /// Add a stream with custom configuration
    pub fn with_stream(mut self, stream: StreamDefinition) -> Self {
        self.streams.push(stream);
        self
    }

    /// Configure whether to auto-start nodes
    pub fn auto_start(mut self, enabled: bool) -> Self {
        self.auto_start = enabled;
        self
    }

    /// Configure whether to wait for cluster formation
    pub fn wait_for_formation(mut self, enabled: bool) -> Self {
        self.wait_for_formation = enabled;
        self
    }

    /// Set cluster formation timeout
    pub fn formation_timeout(mut self, timeout: Duration) -> Self {
        self.formation_timeout = timeout;
        self
    }

    /// Configure a specific node
    pub fn configure_node(mut self, index: usize, config: NodeConfig) -> Self {
        self.node_configs.insert(index, config);
        self
    }

    /// Enable monitoring (Prometheus)
    pub fn with_monitoring(mut self, enabled: bool) -> Self {
        self.enable_monitoring = enabled;
        self
    }

    /// Build the test cluster
    pub async fn build(self) -> Result<TestClusterResult, Box<dyn std::error::Error>> {
        // Generate ports and keys for all nodes
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();
        for _ in 0..self.node_count {
            ports.push(proven_util::port_allocator::allocate_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        // Create attestor and governance
        let attestor = Arc::new(MockAttestor::new());
        let version = Version::from_pcrs(attestor.pcrs_sync());
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![version],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        // Create nodes and consensus instances
        let mut engines = Vec::new();
        let mut clients = Vec::new();
        let mut nodes = Vec::new();
        let mut temp_dirs = Vec::new();

        for i in 0..self.node_count {
            let port = ports[i];
            let signing_key = &signing_keys[i];
            let _node_id = NodeId::new(signing_key.verifying_key());

            // Get custom node config if exists
            let node_config = self.node_configs.get(&i).cloned().unwrap_or_default();

            // Create governance node
            let node = GovernanceNode {
                availability_zone: node_config
                    .availability_zone
                    .unwrap_or_else(|| format!("test-az-{}", i % 3)),
                origin: format!("http://localhost:{}", port),
                public_key: signing_key.verifying_key(),
                region: node_config
                    .region
                    .unwrap_or_else(|| format!("test-region-{}", i % 2)),
                specializations: node_config.specializations,
            };

            governance.add_node(node.clone())?;
            nodes.push(Node::from(node));

            // Determine transport config
            let transport_config = match &self.transport {
                TransportType::Tcp => TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{}", port).parse()?,
                },
                TransportType::WebSocket => TransportConfig::WebSocket,
            };

            // Determine storage config
            let storage_config = match &self.storage {
                StorageType::Memory => StorageConfig::Memory,
                StorageType::RocksDB => {
                    let temp_dir = tempfile::tempdir()?;
                    let path = temp_dir.path().to_path_buf();
                    temp_dirs.push(temp_dir);
                    StorageConfig::RocksDB { path }
                }
            };

            // Configure monitoring
            let mut monitoring_config = MonitoringConfig::default();
            monitoring_config.prometheus.enabled = self.enable_monitoring;

            // Configure groups
            let mut groups_config = GroupsConfig::default();
            groups_config.initial_groups = self.default_groups as u32;

            // Create mock transport - using the parent module's MockTransport
            let transport = super::MockTransport::new(signing_key.clone());

            // Create topology manager
            let topology_manager = Arc::new(
                TopologyManager::new(governance.clone(), NodeId::new(signing_key.verifying_key()))
                    .await?,
            );

            // Create network manager
            let network_manager = Arc::new(NetworkManager::new(
                NodeId::new(signing_key.verifying_key()),
                transport,
                topology_manager.clone(),
            ));

            // Build engine and client
            let (engine, client) = EngineBuilder::new()
                .network_manager(network_manager.clone())
                .topology_manager(topology_manager)
                .governance(governance.clone())
                .signing_key(signing_key.clone())
                .raft_config(openraft::Config::default())
                .transport_config(transport_config)
                .storage_config(storage_config)
                .cluster_join_retry_config(
                    proven_consensus::config::ClusterJoinRetryConfig::default(),
                )
                .global_config(GlobalConsensusConfig::default())
                .groups_config(groups_config)
                .allocation_config(AllocationConfig::default())
                .migration_config(MigrationConfig::default())
                .monitoring_config(monitoring_config)
                .stream_storage_backend(Default::default())
                .build()
                .await?;

            engines.push(engine);
            clients.push(client);
        }

        // Create the test cluster
        let cluster = super::TestCluster {
            engines,
            clients,
            governance,
            nodes,
            ports,
            signing_keys,
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        // Transport is already initialized via NetworkManager
        println!("📡 Transport already initialized via NetworkManager");

        // Start nodes if requested
        if self.auto_start {
            // For single node, start the engine
            if cluster.engines.len() == 1 {
                println!("🔍 Starting single node cluster...");
                cluster.engines[0].clone().start().await?;
                println!("  ✅ Single node completed startup");
            } else {
                // For multi-node clusters, start all engines in parallel
                println!("🔍 Starting all nodes...");
                let start_futures: Vec<_> = cluster
                    .engines
                    .iter()
                    .enumerate()
                    .filter_map(|(i, engine)| {
                        let node_config = self.node_configs.get(&i);
                        if node_config.map(|c| !c.start_offline).unwrap_or(true) {
                            let engine = engine.clone();
                            Some(async move { engine.start().await.map_err(|e| format!("{}", e)) })
                        } else {
                            None
                        }
                    })
                    .collect();

                let results = futures::future::join_all(start_futures).await;

                // Check for errors
                for (i, result) in results.iter().enumerate() {
                    match result {
                        Ok(_) => println!("  ✅ Node {} started", i),
                        Err(e) => {
                            println!("  ❌ Node {} failed to start: {}", i, e);
                            return Err(format!("Node {} failed to start: {}", i, e).into());
                        }
                    }
                }

                // Give nodes time to establish connections
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // Wait for cluster formation if requested
            if self.wait_for_formation {
                let formed = cluster
                    .wait_for_cluster_formation(self.formation_timeout.as_secs())
                    .await;
                if !formed {
                    return Err("Cluster failed to form within timeout".into());
                }
            }
        }

        // Get the first engine and client for creating groups and streams
        let mut group_ids = Vec::new();

        if let (Some(engine), Some(client)) = (cluster.engines.first(), cluster.clients.first()) {
            // The default group should already exist from cluster initialization
            // Just collect the existing groups
            let global_state = engine.global_state();
            let existing_groups = global_state.get_all_groups().await;
            group_ids.extend(existing_groups.iter().map(|g| g.id));

            // Create additional groups if needed
            let groups_to_create = self.default_groups.saturating_sub(existing_groups.len());
            for i in 0..groups_to_create {
                let group_id = ConsensusGroupId::new((existing_groups.len() + i + 1) as u32);
                let members = vec![cluster.nodes[0].node_id().clone()]; // Start with just the first node

                client.create_group(group_id, members).await?;
                group_ids.push(group_id);
            }

            // Create streams
            for (i, stream_def) in self.streams.iter().enumerate() {
                let _group_id = stream_def.group_id.unwrap_or_else(|| {
                    // Round-robin allocation
                    if group_ids.is_empty() {
                        ConsensusGroupId::new(1) // Default group
                    } else {
                        group_ids[i % group_ids.len()]
                    }
                });

                // Note: The group_id will be automatically assigned by create_stream
                // The builder just ensures streams are created after groups
                client
                    .create_stream(
                        stream_def.name.clone(),
                        stream_def.config.clone(),
                        stream_def.group_id,
                    )
                    .await?;

                // Add subscriptions using the client's subscribe method
                for pattern in &stream_def.subscriptions {
                    client
                        .subscribe(&stream_def.name, pattern.clone(), None)
                        .await?;
                }
            }
        }

        Ok(TestClusterResult {
            cluster,
            temp_dirs,
            streams: self.streams,
            group_ids,
        })
    }
}
