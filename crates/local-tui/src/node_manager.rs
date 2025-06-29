//! Node lifecycle management

use crate::ip_allocator::allocate_port;
use crate::messages::{NodeCommand, NodeOperation, TuiNodeConfig};
use crate::node_id::NodeId;

use anyhow::Result;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_governance::TopologyNode;
use proven_governance_mock::MockGovernance;
use proven_local::{Node, NodeStatus};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};
use url::Url;

/// Handle to a running node with its metadata and command processing
pub struct NodeHandle {
    /// Display name (publicly accessible for TUI display)
    pub name: String,

    /// Command sender for this specific node
    pub command_sender: mpsc::Sender<NodeOperation>,

    /// Status of the node - updated by command processor
    pub status: Arc<RwLock<NodeStatus>>,
}

impl NodeHandle {
    /// Create a new node handle with dedicated command processing
    #[must_use]
    pub fn new(
        id: NodeId,
        name: String,
        config: TuiNodeConfig,
        governance: Arc<MockGovernance>,
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
        thread::spawn(move || {
            Self::command_processor(
                id,
                &name_clone,
                &config,
                &command_receiver,
                &governance,
                &status_clone,
            );
        });

        Self {
            name,
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
    #[allow(clippy::cognitive_complexity)]
    fn command_processor(
        id: NodeId,
        name: &str,
        config: &TuiNodeConfig,
        command_receiver: &mpsc::Receiver<NodeOperation>,
        governance: &Arc<MockGovernance>,
        status: &Arc<RwLock<NodeStatus>>,
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
        let mut node = Node::new(config.clone());

        info!("Started command processor for node {} ({})", name, id);

        // Use recv_timeout to periodically sync status
        loop {
            // Try to receive a command with timeout
            let operation = match command_receiver.recv_timeout(Duration::from_millis(250)) {
                Ok(op) => Some(op),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Timeout - sync status and continue
                    Self::sync_node_status(&runtime, &node, status);
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
                NodeOperation::Start { config: new_config } => {
                    if let Some(new_config) = new_config {
                        node = Node::new(*new_config);
                    }

                    Self::handle_start_operation(&runtime, &mut node, id, name, governance, status);
                }
                NodeOperation::Stop => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                }
                NodeOperation::Restart => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                    // Wait a moment for stop to complete
                    thread::sleep(Duration::from_millis(500));
                    Self::handle_start_operation(&runtime, &mut node, id, name, governance, status);
                }
                NodeOperation::Shutdown => {
                    Self::handle_stop_operation(&runtime, &mut node, id, name, governance, status);
                    break;
                }
            }
        }

        info!("Command processor for node {} ({}) shutting down", name, id);
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    /// Sync `NodeHandle` status with Node status
    fn sync_node_status(
        runtime: &tokio::runtime::Runtime,
        node: &Node<MockGovernance>,
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
        node: &mut Node<MockGovernance>,
        id: NodeId,
        name: &str,
        governance: &Arc<MockGovernance>,
        status: &Arc<RwLock<NodeStatus>>,
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

        let topology_node = TopologyNode {
            availability_zone: "local".to_string(),
            origin,
            public_key: hex::encode(public_key.to_bytes()),
            region: "local".to_string(),
            specializations: HashSet::new(),
        };

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
    }

    /// Handle stop operation using the Node's built-in stop method
    fn handle_stop_operation(
        runtime: &tokio::runtime::Runtime,
        node: &mut Node<MockGovernance>,
        id: NodeId,
        name: &str,
        governance: &Arc<MockGovernance>,
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
            let public_key = hex::encode(node.config().node_key.verifying_key().to_bytes());
            if let Err(e) = governance.remove_node(&public_key) {
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

/// Manages multiple node instances
#[derive(Clone)]
pub struct NodeManager {
    nodes: Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
    governance: Arc<MockGovernance>,
    session_id: String,
}

impl NodeManager {
    /// Create a new node manager
    #[must_use]
    pub fn new(governance: Arc<MockGovernance>, session_id: String) -> Self {
        info!("Created new NodeManager with session_id: {}", session_id);

        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            governance,
            session_id,
        }
    }

    /// Get current node information for UI display
    ///
    /// This method reads the status directly from each `NodeHandle` synchronously.
    pub fn get_nodes_for_ui(&self) -> HashMap<NodeId, (String, NodeStatus)> {
        let nodes = self.nodes.read();
        let mut result = HashMap::new();

        for (id, handle) in nodes.iter() {
            let name = handle.name.clone();
            // Get the status directly from the `NodeHandle` (synchronous)
            let status = handle.get_status();
            result.insert(*id, (name, status));
        }

        drop(nodes);

        result
    }

    /// Check if all nodes have been shut down (for shutdown coordination)
    #[allow(clippy::cognitive_complexity)]
    pub fn is_shutdown_complete(&self) -> bool {
        let nodes = self.nodes.read();

        // If no nodes exist, shutdown is complete
        if nodes.is_empty() {
            info!("Shutdown complete: no nodes tracked");
            return true;
        }

        // Check if all nodes are in Stopped or Failed state
        let mut running_nodes = Vec::new();
        for (id, handle) in nodes.iter() {
            let status = handle.get_status();
            match status {
                NodeStatus::Stopped | NodeStatus::Failed(_) => {
                    info!("Node {} is shutdown: {}", id.full_pokemon_name(), status);
                }
                _ => {
                    info!("Node {} still running: {}", id.full_pokemon_name(), status);
                    running_nodes.push((*id, status));
                }
            }
        }

        let total_nodes = nodes.len();
        drop(nodes);

        if running_nodes.is_empty() {
            info!("Shutdown complete: all {} nodes are stopped", total_nodes);
            true
        } else {
            info!(
                "Shutdown incomplete: {} nodes still running",
                running_nodes.len()
            );
            false
        }
    }

    /// Start the command handler
    pub fn start_command_handler(&self, command_receiver: mpsc::Receiver<NodeCommand>) {
        let nodes = self.nodes.clone();
        let governance = self.governance.clone();
        let session_id = self.session_id.clone();

        thread::spawn(move || {
            while let Ok(command) = command_receiver.recv() {
                match command {
                    NodeCommand::StartNode { id, name, config } => {
                        Self::handle_start_node(
                            &nodes,
                            &governance,
                            &session_id,
                            id,
                            &name,
                            config,
                        );
                    }
                    NodeCommand::StopNode { id } => {
                        Self::handle_stop_node(&nodes, &governance, id);
                    }
                    NodeCommand::RestartNode { id } => {
                        Self::handle_restart_node(&nodes.clone(), &governance, &session_id, id);
                    }
                    NodeCommand::Shutdown => {
                        Self::handle_shutdown(&nodes);
                        break;
                    }
                }
            }
        });
    }

    fn handle_start_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        governance: &Arc<MockGovernance>,
        session_id: &str,
        id: NodeId,
        name: &str,
        config: Option<Box<TuiNodeConfig>>,
    ) {
        let node_config = config.map_or_else(
            || Self::create_node_config(id, name, governance, session_id),
            |config| *config,
        );

        // Check if node already exists
        {
            let nodes_guard = nodes.read();
            if let Some(existing_node) = nodes_guard.get(&id) {
                // Node exists, send start command to it (non-blocking!)
                if let Err(e) = existing_node.send_command(NodeOperation::Start {
                    config: Some(Box::new(node_config)),
                }) {
                    error!(
                        "Failed to send start command to existing node {} ({}): {}",
                        name, id, e
                    );
                }
                return;
            }
        }

        // Create new node handle with command processing
        let node_handle = NodeHandle::new(
            id,
            name.to_string(),
            node_config.clone(),
            governance.clone(),
        );

        // Add to nodes map
        nodes.write().insert(id, node_handle);

        // Send start command to the newly created node (non-blocking!)
        {
            let nodes_guard = nodes.read();
            if let Some(node_handle) = nodes_guard.get(&id) {
                if let Err(e) = node_handle.send_command(NodeOperation::Start {
                    config: Some(Box::new(node_config)),
                }) {
                    error!(
                        "Failed to send start command to node {} ({}): {}",
                        name, id, e
                    );
                }
            }
        }
    }

    fn handle_stop_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        _governance: &Arc<MockGovernance>,
        id: NodeId,
    ) {
        let nodes_guard = nodes.read();
        if let Some(node_handle) = nodes_guard.get(&id) {
            // Send stop command to the node (non-blocking!)
            if let Err(e) = node_handle.send_command(NodeOperation::Stop) {
                error!("Failed to send stop command to node {}: {}", id, e);
            }
        } else {
            warn!("Attempted to stop non-existent node: {}", id);
        }
    }

    fn handle_restart_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        _governance: &Arc<MockGovernance>,
        _session_id: &str,
        id: NodeId,
    ) {
        let nodes_guard = nodes.read();
        if let Some(node_handle) = nodes_guard.get(&id) {
            // Send restart command to the node (non-blocking!)
            if let Err(e) = node_handle.send_command(NodeOperation::Restart) {
                error!("Failed to send restart command to node {}: {}", id, e);
            }
        } else {
            warn!("Attempted to restart non-existent node: {}", id);
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_shutdown(nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>) {
        info!("Shutting down all nodes...");

        // Send shutdown commands to all nodes (non-blocking!)
        // Leave nodes in HashMap so main loop can monitor their status
        {
            let nodes_guard = nodes.read();
            for (id, node_handle) in nodes_guard.iter() {
                info!(
                    "Sending shutdown command to node {}",
                    id.full_pokemon_name()
                );
                if let Err(e) = node_handle.send_command(NodeOperation::Shutdown) {
                    error!("Failed to send shutdown command to node {}: {}", id, e);
                }
            }
        }

        info!("Shutdown commands sent to all nodes - waiting for graceful shutdown");
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
