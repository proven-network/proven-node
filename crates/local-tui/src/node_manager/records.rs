//! Node record types and handles for managing node lifecycle

use crate::ip_allocator::allocate_port;
use crate::messages::{NodeOperation, TuiNodeConfig};
use crate::node_id::TuiNodeId;

use anyhow::Result;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_local::{LocalNode, NodeStatus};
use proven_topology::{Node, NodeId};
use proven_topology_mock::MockTopologyAdaptor;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use tracing::{error, info, warn};
use url::Url;

/// Message sent from `NodeHandle` back to `NodeManager` when a node finishes
#[derive(Debug)]
pub enum NodeManagerMessage {
    NodeFinished {
        id: TuiNodeId,
        name: String,
        final_status: NodeStatus,
        specializations: HashSet<proven_topology::NodeSpecialization>,
    },
}

/// Record of a node in the manager - either currently running or stopped
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // TODO: Box NodeHandle
pub enum NodeRecord {
    Running(NodeHandle),
    Stopped {
        _id: TuiNodeId,
        name: String,
        final_status: NodeStatus,
        specializations: HashSet<proven_topology::NodeSpecialization>,
        _stopped_at: std::time::Instant,
    },
}

/// Handle to a running node with its metadata and command processing
#[derive(Debug)]
pub struct NodeHandle {
    /// Node identifier
    pub _id: TuiNodeId,

    /// Display name (publicly accessible for TUI display)
    pub name: String,

    /// Node configuration (stored for URL extraction)
    pub config: TuiNodeConfig,

    /// Specializations for this node
    pub specializations: HashSet<proven_topology::NodeSpecialization>,

    /// Command sender for this specific node
    pub command_sender: mpsc::Sender<NodeOperation>,

    /// Status of the node - updated by command processor
    pub status: Arc<RwLock<NodeStatus>>,
}

impl NodeHandle {
    /// Create a new node handle with dedicated command processing
    #[must_use]
    pub fn new(
        id: TuiNodeId,
        name: String,
        config: TuiNodeConfig,
        specializations: HashSet<proven_topology::NodeSpecialization>,
        governance: Arc<MockTopologyAdaptor>,
        completion_sender: mpsc::Sender<NodeManagerMessage>,
    ) -> Self {
        let (command_sender, command_receiver) = mpsc::channel();

        // Create dedicated runtime for this node
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(format!("node-{}-{}", id.execution_order(), id.pokemon_id()))
            .enable_all()
            .build()
        {
            Ok(runtime) => Some(runtime),
            Err(e) => {
                error!("Failed to create runtime for node {}: {}", name, e);
                None
            }
        };

        // Shared status that will be updated by the command processor
        let status = Arc::new(RwLock::new(NodeStatus::NotStarted));

        // Spawn dedicated command processing thread for this node
        let name_clone = name.clone();
        let status_clone = status.clone();
        let config_clone = config.clone();
        let specializations_clone = specializations.clone();
        thread::spawn(move || {
            Self::command_processor(
                id,
                &name_clone,
                &config_clone,
                &command_receiver,
                &governance,
                &status_clone,
                &completion_sender,
                specializations_clone,
            );
        });

        Self {
            _id: id,
            name,
            config,
            specializations,
            command_sender,
            status,
        }
    }

    /// Send a command to this node's command processor
    pub fn send_command(
        &self,
        operation: NodeOperation,
    ) -> Result<(), mpsc::SendError<NodeOperation>> {
        self.command_sender.send(operation)
    }

    /// Get the current status of the node (synchronous - no async/runtime needed)
    pub fn get_status(&self) -> NodeStatus {
        self.status.read().clone()
    }

    /// Command processor that runs in a dedicated thread for each node
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::cognitive_complexity)]
    fn command_processor(
        id: TuiNodeId,
        name: &str,
        config: &TuiNodeConfig,
        command_receiver: &mpsc::Receiver<NodeOperation>,
        governance: &Arc<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
        completion_sender: &mpsc::Sender<NodeManagerMessage>,
        specializations: HashSet<proven_topology::NodeSpecialization>,
    ) {
        // Create runtime for this command processor
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name(format!("node-{}-{}", id.execution_order(), id.pokemon_id()))
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => {
                error!("Failed to create command runtime for node {}: {}", name, e);
                return;
            }
        };

        // Create node instance using the Node's built-in state management
        let mut node = LocalNode::new(config.clone());

        info!("Started command processor for node {} ({})", name, id);

        // Use recv_timeout to periodically sync status
        loop {
            // Try to receive a command with timeout
            let operation = match command_receiver.recv_timeout(Duration::from_millis(250)) {
                Ok(op) => Some(op),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Timeout - sync status and continue
                    Self::sync_node_status(&runtime, &node, status);

                    // Check for terminal states that should end the command processor
                    let current_status = status.read().clone();
                    if matches!(current_status, NodeStatus::Failed(_) | NodeStatus::Stopped) {
                        info!(
                            "Node {} ({}) is in terminal state {:?}, shutting down command processor",
                            name, id, current_status
                        );
                        break;
                    }
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel disconnected - exit
                    break;
                }
            };

            let Some(operation) = operation else {
                break;
            };
            match operation {
                NodeOperation::Start {
                    config: new_config,
                    specializations,
                } => {
                    if let Some(new_config) = new_config {
                        node = LocalNode::new(*new_config);
                    }

                    Self::handle_start_operation(
                        &runtime,
                        &mut node,
                        id,
                        name,
                        governance,
                        status,
                        specializations,
                    );
                }
                NodeOperation::Stop => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                }
                NodeOperation::Restart => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                    // Wait a moment for stop to complete
                    thread::sleep(Duration::from_millis(500));
                    Self::handle_start_operation(
                        &runtime, &mut node, id, name, governance, status, None,
                    );
                }
                NodeOperation::Shutdown => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                    break;
                }
            }

            // Check if node has failed and break out of loop to send completion message
            {
                let current_status = status.read().clone();
                if matches!(current_status, NodeStatus::Failed(_)) {
                    info!(
                        "Node {} ({}) has failed, shutting down command processor",
                        name, id
                    );
                    break;
                }
            }
        }

        info!("Command processor for node {} ({}) shutting down", name, id);

        // Send completion message to NodeManager
        let final_status = status.read().clone();
        let completion_message = NodeManagerMessage::NodeFinished {
            id,
            name: name.to_string(),
            final_status,
            specializations,
        };

        if let Err(e) = completion_sender.send(completion_message) {
            warn!("Failed to send completion message for node {}: {}", name, e);
        }

        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    /// Sync `NodeHandle` status with Node status
    fn sync_node_status(
        runtime: &tokio::runtime::Runtime,
        node: &LocalNode<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
    ) {
        let current_node_status = runtime.block_on(async { node.status().await });

        // Update NodeHandle status to match Node status
        {
            let mut status_guard = status.write();
            if *status_guard != current_node_status {
                *status_guard = current_node_status;
            }
        }
    }

    /// Handle start operation using the Node's built-in start method
    #[allow(clippy::cognitive_complexity)]
    fn handle_start_operation(
        runtime: &tokio::runtime::Runtime,
        node: &mut LocalNode<MockTopologyAdaptor>,
        id: TuiNodeId,
        name: &str,
        governance: &Arc<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
        specializations: Option<HashSet<proven_topology::NodeSpecialization>>,
    ) {
        // Update status to Starting immediately
        {
            let mut status_guard = status.write();
            *status_guard = NodeStatus::Starting;
        }

        // Check current node status first
        let current_status = runtime.block_on(node.status());
        match current_status {
            NodeStatus::Starting | NodeStatus::Running => {
                info!(
                    "Node {} ({}) is already starting/running, ignoring start command",
                    name, id
                );
                // Update our status to match the node's actual status
                {
                    let mut status_guard = status.write();
                    *status_guard = current_status;
                }
                return;
            }
            _ => {}
        }

        // Add to governance before starting
        let public_key = node.config().node_key.verifying_key();
        let actual_port = node.config().port;

        let origin = if id.is_first_node() {
            format!("http://localhost:{actual_port}")
        } else {
            let pokemon_name = id.pokemon_name();
            format!("http://{pokemon_name}.local:{actual_port}")
        };

        let node_specializations = specializations.unwrap_or_default();

        let topology_node = Node::new(
            "local".to_string(),
            origin,
            NodeId::from(public_key),
            "local".to_string(),
            node_specializations,
        );

        // Add to governance
        if let Err(e) = governance.add_node(topology_node) {
            error!("Failed to add node to governance: {}", e);
            // Update status to failed
            {
                let mut status_guard = status.write();
                *status_guard = NodeStatus::Failed(format!("Failed to add to governance: {e}"));
            }
            return;
        }

        // Start the node using its built-in start method
        let start_result = runtime.block_on(async { node.start().await });

        if let Err(e) = start_result {
            error!("Failed to start node {} ({}): {}", name, id, e);
            {
                let mut status_guard = status.write();
                *status_guard = NodeStatus::Failed(format!("Start failed: {e}"));
            }
            return;
        }

        info!("Node {} ({}) start command completed", name, id);

        // Sync status with actual node after start operation to ensure UI reflects true state
        Self::sync_node_status(runtime, node, status);
    }

    /// Handle stop operation using the Node's built-in stop method
    fn handle_stop_operation(
        runtime: &tokio::runtime::Runtime,
        node: &mut LocalNode<MockTopologyAdaptor>,
        id: TuiNodeId,
        name: &str,
        governance: &Arc<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
    ) {
        // Update status to Stopping immediately
        {
            let mut status_guard = status.write();
            *status_guard = NodeStatus::Stopping;
        }

        let result = runtime.block_on(async {
            // Check current status using Node's built-in status method
            let current_status = node.status().await;
            match current_status {
                NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Stopping => {
                    info!(
                        "Node {} ({}) is not running, ignoring stop command",
                        name, id
                    );
                    return Ok(current_status);
                }
                _ => {}
            }

            // Remove from governance before stopping
            let public_key = node.config().node_key.verifying_key();
            if let Err(e) = governance.remove_node(public_key) {
                warn!("Failed to remove node {} from governance: {}", id, e);
            }

            // Stop the node using its built-in stop method
            let stop_result = node.stop().await;

            if let Err(e) = stop_result {
                error!("Failed to stop node {} ({}): {}", name, id, e);
                return Err(e);
            }

            // Check the actual status after stopping
            let final_status = node.status().await;
            Ok(final_status)
        });

        // Update status based on result
        match result {
            Ok(final_status) => {
                info!("Node {} ({}) stop completed successfully", name, id);
                {
                    let mut status_guard = status.write();
                    *status_guard = final_status;
                }
            }
            Err(e) => {
                error!("Failed to stop node {} ({}): {}", name, id, e);
                {
                    let mut status_guard = status.write();
                    *status_guard = NodeStatus::Failed(format!("Stop failed: {e}"));
                }
            }
        }
    }
}

/// Create a default node configuration
pub fn create_node_config(
    _id: TuiNodeId,
    name: &str,
    governance: &Arc<MockTopologyAdaptor>,
    session_id: &str,
) -> proven_local::NodeConfig<proven_topology_mock::MockTopologyAdaptor> {
    let main_port = allocate_port().unwrap_or_else(|e| {
        error!("Failed to allocate port for node {}: {}", name, e);
        3000 // Fallback port
    });

    let private_key = SigningKey::generate(&mut rand::thread_rng());

    build_node_config(name, main_port, governance, private_key, session_id)
}

/// Build a complete node configuration
#[allow(clippy::too_many_lines)]
pub fn build_node_config(
    name: &str,
    main_port: u16,
    governance: &Arc<MockTopologyAdaptor>,
    private_key: SigningKey,
    session_id: &str,
) -> proven_local::NodeConfig<proven_topology_mock::MockTopologyAdaptor> {
    TuiNodeConfig {
        allow_single_node: false,
        bitcoin_mainnet_fallback_rpc_endpoint: Url::parse("https://bitcoin-rpc.publicnode.com")
            .unwrap(),
        bitcoin_mainnet_proxy_port: allocate_port().unwrap(),
        bitcoin_mainnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/bitcoin-mainnet"
        )),
        bitcoin_mainnet_rpc_port: allocate_port().unwrap(),

        bitcoin_testnet_fallback_rpc_endpoint: Url::parse(
            "https://bitcoin-testnet-rpc.publicnode.com",
        )
        .unwrap(),
        bitcoin_testnet_proxy_port: allocate_port().unwrap(),
        bitcoin_testnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/bitcoin-testnet"
        )),
        bitcoin_testnet_rpc_port: allocate_port().unwrap(),

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
        ethereum_mainnet_fallback_rpc_endpoint: Url::parse("https://ethereum-rpc.publicnode.com")
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

        network_config_path: None, // Using shared governance instead
        node_key: private_key,

        postgres_bin_path: PathBuf::from("/usr/local/pgsql/bin"),
        postgres_port: allocate_port().unwrap(),
        postgres_skip_vacuum: false,
        postgres_store_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/postgres")),

        radix_mainnet_fallback_rpc_endpoint: Url::parse("https://mainnet.radixdlt.com").unwrap(),
        radix_mainnet_http_port: allocate_port().unwrap(),
        radix_mainnet_p2p_port: allocate_port().unwrap(),
        radix_mainnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/radix-node-mainnet"
        )),

        radix_stokenet_fallback_rpc_endpoint: Url::parse("https://stokenet.radixdlt.com").unwrap(),
        radix_stokenet_http_port: allocate_port().unwrap(),
        radix_stokenet_p2p_port: allocate_port().unwrap(),
        radix_stokenet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/radix-node-stokenet"
        )),
    }
}
