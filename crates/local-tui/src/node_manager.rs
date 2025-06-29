//! Node lifecycle management

use crate::messages::{NodeCommand, NodeStatus, TuiMessage, TuiNodeConfig};
use crate::node_id::NodeId;

use anyhow::Result;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_governance::TopologyNode;
use proven_governance_mock::MockGovernance;
use proven_local::Node;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};
use url::Url;

/// Global port allocator starting from 3000
static NEXT_PORT: LazyLock<Mutex<u16>> = LazyLock::new(|| Mutex::new(3000));

/// Allocate the next available port, starting from 3000
fn allocate_port() -> Result<u16> {
    let mut port_guard = NEXT_PORT.lock().unwrap();

    // Try up to 10000 ports from the current position
    for _ in 0..10000 {
        let port = *port_guard;
        *port_guard += 1;

        // Check if port is actually available on the system
        if is_port_available(port) {
            return Ok(port);
        }
    }

    anyhow::bail!(
        "No available ports found after trying 10000 ports from {}",
        *port_guard - 10000
    )
}

/// Check if a port is available by attempting to bind to it
fn is_port_available(port: u16) -> bool {
    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).is_ok()
}

/// Handle to a running node with its metadata
pub struct NodeHandle {
    /// Node identifier
    pub id: NodeId,
    /// Display name
    pub name: String,
    /// The actual node instance
    pub node: Node<MockGovernance>,
    /// Dedicated runtime for this node
    pub runtime: Option<tokio::runtime::Runtime>,
}

impl NodeHandle {
    /// Create a new node handle
    #[must_use]
    pub fn new(id: NodeId, name: String, config: proven_local::NodeConfig<MockGovernance>) -> Self {
        let node = Node::new(config);
        Self {
            id,
            name,
            node,
            runtime: None,
        }
    }

    /// Start the node
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting node {} ({})", self.name, self.id);

        self.node
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start node: {}", e))
    }
}

/// Manages multiple node instances
#[derive(Clone)]
pub struct NodeManager {
    nodes: Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
    message_sender: mpsc::Sender<TuiMessage>,
    governance: Arc<MockGovernance>,
    session_id: String,
}

impl NodeManager {
    /// Create a new node manager
    #[must_use]
    pub fn new(
        message_sender: mpsc::Sender<TuiMessage>,
        governance: Arc<MockGovernance>,
        session_id: String,
    ) -> Self {
        info!("Created new NodeManager with session_id: {}", session_id);

        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            governance,
            session_id,
        }
    }

    /// Start the command handler
    pub fn start_command_handler(&self, command_receiver: mpsc::Receiver<NodeCommand>) {
        let nodes = self.nodes.clone();
        let message_sender = self.message_sender.clone();
        let governance = self.governance.clone();
        let session_id = self.session_id.clone();

        thread::spawn(move || {
            while let Ok(command) = command_receiver.recv() {
                match command {
                    NodeCommand::StartNode { id, name, config } => {
                        Self::handle_start_node(
                            &nodes,
                            &message_sender,
                            &governance,
                            &session_id,
                            id,
                            name,
                            config,
                        );
                    }
                    NodeCommand::StopNode { id } => {
                        Self::handle_stop_node(&nodes, &message_sender, &governance, id);
                    }
                    NodeCommand::RestartNode { id } => {
                        Self::handle_restart_node(
                            &nodes.clone(),
                            &message_sender.clone(),
                            &governance,
                            &session_id,
                            id,
                        );
                    }
                    NodeCommand::Shutdown => {
                        Self::handle_shutdown(&nodes, &message_sender, &governance);
                        break;
                    }
                }
            }
        });
    }

    fn handle_start_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
        session_id: &str,
        id: NodeId,
        name: String,
        config: Option<Box<TuiNodeConfig>>,
    ) {
        let node_config = config.map_or_else(
            || Self::create_node_config(id, &name, governance, session_id),
            |config| *config,
        );

        // Notify that node is starting
        let _ = message_sender.send(TuiMessage::NodeStatusUpdate {
            id,
            status: NodeStatus::Starting,
        });

        // Create the node handle
        let mut node_handle = NodeHandle::new(id, name.clone(), node_config.clone());

        // Create a dedicated runtime for this node
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(format!("node-{}-{}", id.execution_order(), id.pokemon_id()))
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => {
                error!("Failed to create runtime for node {}: {}", name, e);
                let _ = message_sender.send(TuiMessage::NodeFailed {
                    id,
                    error: format!("Failed to create runtime: {e}"),
                });
                return;
            }
        };

        // Start the node in its dedicated runtime
        let start_result = runtime.block_on(async {
            // Add the node to governance before starting
            let public_key = node_config.node_key.verifying_key();
            let actual_port = node_config.port;

            // Determine the proper origin endpoint based on whether this is the first node
            let origin = if id.is_first_node() {
                format!("http://localhost:{actual_port}")
            } else {
                let pokemon_name = id.pokemon_name();
                format!("http://{pokemon_name}.local:{actual_port}")
            };

            let topology_node = TopologyNode {
                availability_zone: "local".to_string(),
                origin,
                public_key: hex::encode(public_key.to_bytes()),
                region: "local".to_string(),
                specializations: HashSet::new(),
            };

            // Add to governance
            if let Err(e) = governance.add_node(topology_node) {
                return Err(anyhow::anyhow!("Failed to add node to governance: {}", e));
            }

            // Now start the node
            node_handle.start().await
        });

        match start_result {
            Ok(()) => {
                info!(
                    "Node {} ({}) start method completed, waiting for bootstrap...",
                    name, id
                );

                // Store the runtime in the node handle
                node_handle.runtime = Some(runtime);

                // Add to nodes map
                nodes.write().insert(id, node_handle);

                // Send NodeStarted message with the name, but keep status as Starting for now
                let _ = message_sender.send(TuiMessage::NodeStarted { id, name });

                // Start a background task to monitor when the node actually becomes running
                let nodes_clone = nodes.clone();
                let message_sender_clone = message_sender.clone();
                thread::spawn(move || {
                    Self::monitor_node_startup(&nodes_clone, &message_sender_clone, id);
                });
            }
            Err(e) => {
                error!("Failed to start node {} ({}): {}", name, id, e);
                let _ = message_sender.send(TuiMessage::NodeFailed {
                    id,
                    error: e.to_string(),
                });
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_stop_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
        id: NodeId,
    ) {
        let mut nodes_guard = nodes.write();

        if let Some(mut node_handle) = nodes_guard.remove(&id) {
            info!("Stopping node {} ({})", node_handle.name, id);

            // Remove from governance before stopping
            let public_key = hex::encode(
                node_handle
                    .node
                    .config()
                    .node_key
                    .verifying_key()
                    .to_bytes(),
            );
            if let Err(e) = governance.remove_node(&public_key) {
                warn!("Failed to remove node {} from governance: {}", id, e);
            }

            // Notify that the node is stopping
            let _ = message_sender.send(TuiMessage::NodeStatusUpdate {
                id,
                status: NodeStatus::Stopping,
            });

            // Use the runtime to stop the node
            let stop_result = if node_handle.runtime.is_some() {
                // Create a temporary runtime reference to avoid borrow conflicts
                let runtime = node_handle.runtime.take().unwrap();

                // Stop the node's internal components
                let stop_future = node_handle.node.stop();
                let node_stop_result = runtime.block_on(async {
                    tokio::time::timeout(Duration::from_secs(30), stop_future)
                        .await
                        .map_err(|_| anyhow::anyhow!("Node stop timed out"))
                        .and_then(|result| {
                            result.map_err(|e| anyhow::anyhow!("Node stop failed: {}", e))
                        })
                });

                // Shutdown the runtime
                info!(
                    "Shutting down runtime for node {} ({})",
                    node_handle.name, id
                );
                runtime.shutdown_timeout(Duration::from_secs(10));
                info!(
                    "Runtime shutdown complete for node {} ({})",
                    node_handle.name, id
                );

                node_stop_result
            } else {
                warn!(
                    "Node {} ({}) has no runtime, marking as stopped",
                    node_handle.name, id
                );
                Ok(())
            };

            match stop_result {
                Ok(()) => {
                    info!("Node {} ({}) stopped successfully", node_handle.name, id);
                    let _ = message_sender.send(TuiMessage::NodeStopped { id });
                }
                Err(e) => {
                    error!("Failed to stop node {} ({}): {}", node_handle.name, id, e);
                    let _ = message_sender.send(TuiMessage::NodeFailed {
                        id,
                        error: format!("Failed to stop: {e}"),
                    });
                }
            }
        } else {
            warn!("Attempted to stop non-existent node: {}", id);
        }
    }

    fn handle_restart_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
        session_id: &str,
        id: NodeId,
    ) {
        // First stop the node
        Self::handle_stop_node(nodes, message_sender, governance, id);

        // Get the node configuration to restart with
        let (name, config) = {
            let nodes_guard = nodes.read();
            if let Some(node_handle) = nodes_guard.get(&id) {
                (node_handle.name.clone(), node_handle.node.config().clone())
            } else {
                error!("Cannot restart non-existent node: {}", id);
                return;
            }
        };

        // Then start it again
        Self::handle_start_node(
            nodes,
            message_sender,
            governance,
            session_id,
            id,
            name,
            Some(Box::new(config)),
        );
    }

    fn handle_shutdown(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
    ) {
        info!("Shutting down all nodes...");

        let node_ids: Vec<NodeId> = {
            let nodes_guard = nodes.read();
            nodes_guard.keys().copied().collect()
        };

        for id in node_ids {
            Self::handle_stop_node(nodes, message_sender, governance, id);
        }

        // Clear the nodes map
        nodes.write().clear();

        let _ = message_sender.send(TuiMessage::ShutdownComplete);
        info!("All nodes shut down successfully");
    }

    fn create_node_config(
        _id: NodeId,
        name: &str,
        governance: &Arc<MockGovernance>,
        session_id: &str,
    ) -> proven_local::NodeConfig<proven_governance_mock::MockGovernance> {
        let main_port = allocate_port().unwrap_or_else(|e| {
            error!("Failed to allocate port for node {}: {}", name, e);
            3000 // Fallback port
        });

        let private_key = SigningKey::generate(&mut rand::thread_rng());

        Self::build_node_config(name, main_port, governance, private_key, session_id)
    }

    /// Monitor a node's startup process and send status updates
    #[allow(clippy::cognitive_complexity)]
    fn monitor_node_startup(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
        id: NodeId,
    ) {
        loop {
            thread::sleep(Duration::from_millis(100));

            let current_status = {
                let nodes_guard = nodes.read();
                if let Some(node_handle) = nodes_guard.get(&id) {
                    node_handle.runtime.as_ref().map_or_else(
                        || NodeStatus::Failed("No runtime".to_string()),
                        |runtime| runtime.block_on(async { node_handle.node.status().await }),
                    )
                } else {
                    // Node was removed, stop monitoring
                    return;
                }
            };

            match current_status {
                NodeStatus::Running => {
                    info!("Node {} is now running", id.pokemon_name());
                    let _ = message_sender.send(TuiMessage::NodeStatusUpdate {
                        id,
                        status: NodeStatus::Running,
                    });
                    return; // Stop monitoring
                }
                NodeStatus::Failed(ref error) => {
                    error!(
                        "Node {} failed during startup: {}",
                        id.pokemon_name(),
                        error
                    );
                    let _ = message_sender.send(TuiMessage::NodeFailed {
                        id,
                        error: error.clone(),
                    });
                    return; // Stop monitoring
                }
                NodeStatus::Stopped => {
                    warn!("Node {} stopped during startup", id.pokemon_name());
                    let _ = message_sender.send(TuiMessage::NodeStopped { id });
                    return; // Stop monitoring
                }
                NodeStatus::Starting | NodeStatus::NotStarted => {
                    // Continue monitoring
                }
                NodeStatus::Stopping => {
                    warn!("Node {} is stopping during startup", id.pokemon_name());
                    let _ = message_sender.send(TuiMessage::NodeStatusUpdate {
                        id,
                        status: NodeStatus::Stopping,
                    });
                    // Continue monitoring to see final state
                }
            }
        }
    }

    pub fn build_node_config(
        name: &str,
        main_port: u16,
        governance: &Arc<MockGovernance>,
        private_key: SigningKey,
        session_id: &str,
    ) -> proven_local::NodeConfig<proven_governance_mock::MockGovernance> {
        TuiNodeConfig {
            allow_single_node: false,
            bitcoin_mainnet_fallback_rpc_endpoint: Url::parse("https://bitcoin-rpc.publicnode.com")
                .unwrap(),
            bitcoin_mainnet_proxy_port: allocate_port().unwrap(),
            bitcoin_mainnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/bitcoin-mainnet"
            )),

            bitcoin_testnet_fallback_rpc_endpoint: Url::parse(
                "https://bitcoin-testnet-rpc.publicnode.com",
            )
            .unwrap(),
            bitcoin_testnet_proxy_port: allocate_port().unwrap(),
            bitcoin_testnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/bitcoin-testnet"
            )),

            ethereum_holesky_consensus_http_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-holesky/lighthouse"
            )),

            ethereum_holesky_execution_discovery_port: allocate_port().unwrap(),
            ethereum_holesky_execution_http_port: allocate_port().unwrap(),
            ethereum_holesky_execution_metrics_port: allocate_port().unwrap(),
            ethereum_holesky_execution_rpc_port: allocate_port().unwrap(),
            ethereum_holesky_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-holesky/reth"
            )),
            ethereum_holesky_fallback_rpc_endpoint: Url::parse(
                "https://ethereum-holesky-rpc.publicnode.com",
            )
            .unwrap(),

            ethereum_mainnet_consensus_http_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-mainnet/lighthouse"
            )),

            ethereum_mainnet_execution_discovery_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_http_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_metrics_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_rpc_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-mainnet/reth"
            )),
            ethereum_mainnet_fallback_rpc_endpoint: Url::parse(
                "https://ethereum-rpc.publicnode.com",
            )
            .unwrap(),

            ethereum_sepolia_consensus_http_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-sepolia/lighthouse"
            )),

            ethereum_sepolia_execution_discovery_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_http_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_metrics_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_rpc_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/ethereum-sepolia/reth"
            )),
            ethereum_sepolia_fallback_rpc_endpoint: Url::parse(
                "https://ethereum-sepolia-rpc.publicnode.com",
            )
            .unwrap(),

            governance: (**governance).clone(), // Use the shared governance instance
            port: main_port,

            nats_bin_dir: None,
            nats_client_port: allocate_port().unwrap(),
            nats_cluster_port: allocate_port().unwrap(),
            nats_config_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/nats-config"
            )),
            nats_http_port: allocate_port().unwrap(),
            nats_store_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/nats")),

            network_config_path: None, // Using shared governance instead
            node_key: private_key,

            postgres_bin_path: PathBuf::from("/usr/local/pgsql/bin"),
            postgres_port: allocate_port().unwrap(),
            postgres_skip_vacuum: false,
            postgres_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/postgres"
            )),

            radix_mainnet_fallback_rpc_endpoint: Url::parse("https://mainnet.radixdlt.com")
                .unwrap(),
            radix_mainnet_http_port: allocate_port().unwrap(),
            radix_mainnet_p2p_port: allocate_port().unwrap(),
            radix_mainnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/radix-node-mainnet"
            )),

            radix_stokenet_fallback_rpc_endpoint: Url::parse("https://stokenet.radixdlt.com")
                .unwrap(),
            radix_stokenet_http_port: allocate_port().unwrap(),
            radix_stokenet_p2p_port: allocate_port().unwrap(),
            radix_stokenet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{session_id}/data/{name}/radix-node-stokenet"
            )),
        }
    }
}
