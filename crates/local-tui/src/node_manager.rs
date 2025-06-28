//! Node lifecycle management

use crate::messages::{NodeCommand, NodeStatus, TuiMessage, TuiNodeConfig};
use crate::node_id::NodeId;

use anyhow::Result;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_governance::TopologyNode;
use proven_governance_mock::MockGovernance;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

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

/// Handle to a running node
#[derive(Debug)]
pub struct NodeHandle {
    /// Node identifier
    pub id: NodeId,
    /// Display name
    pub name: String,
    /// Node configuration
    pub config: TuiNodeConfig,
    /// Current status
    pub status: NodeStatus,
    /// Cancellation token for shutdown
    pub shutdown_token: CancellationToken,
    /// Task handle for the node
    pub task_handle: Option<tokio::task::JoinHandle<Result<(), proven_local::Error>>>,
    /// Dedicated runtime for this node
    pub runtime: Option<tokio::runtime::Runtime>,
}

impl NodeHandle {
    /// Create a new node handle in Starting state
    #[must_use]
    pub fn new(id: NodeId, name: String, config: TuiNodeConfig) -> Self {
        Self {
            id,
            name,
            config,
            status: NodeStatus::Starting,
            shutdown_token: CancellationToken::new(),
            task_handle: None,
            runtime: None,
        }
    }

    /// Update the status of this node
    pub fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    /// Stop the node
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to shut down within the timeout period.
    pub fn stop(&mut self) -> Result<()> {
        // If the node is already stopped or failed, skip graceful shutdown
        if matches!(self.status, NodeStatus::Stopped | NodeStatus::Failed(_)) {
            info!(
                "Node {} ({}) already stopped/failed, skipping graceful shutdown",
                self.name, self.id
            );
        } else {
            self.set_status(NodeStatus::Stopping);
            self.shutdown_token.cancel();

            if let Some(handle) = self.task_handle.take() {
                // Give the node some time to shut down gracefully within its own runtime
                if let Some(runtime) = &self.runtime {
                    let shutdown_timeout = Duration::from_secs(30);
                    match runtime
                        .block_on(async { tokio::time::timeout(shutdown_timeout, handle).await })
                    {
                        Ok(join_result) => match join_result {
                            Ok(node_result) => match node_result {
                                Ok(()) => {
                                    info!("Node {} ({}) shut down gracefully", self.name, self.id);
                                }
                                Err(e) => info!(
                                    "Node {} ({}) completed with error during shutdown: {}",
                                    self.name, self.id, e
                                ),
                            },
                            Err(join_error) => {
                                info!(
                                    "Node {} ({}) task panicked during shutdown: {}",
                                    self.name, self.id, join_error
                                );
                            }
                        },
                        Err(_) => {
                            warn!(
                                "Node {} ({}) shutdown timed out after 30 seconds",
                                self.name, self.id
                            );
                        }
                    }
                }
            }
        }

        // Shutdown the dedicated runtime and wait for completion
        if let Some(runtime) = self.runtime.take() {
            info!("Shutting down runtime for node {} ({})", self.name, self.id);

            // Use a blocking shutdown to ensure the runtime fully completes
            // We use spawn_blocking to avoid blocking the current thread if this is called from async context
            let node_name = self.name.clone();
            let node_id = self.id;

            // Block until the runtime is completely shut down
            runtime.shutdown_timeout(Duration::from_secs(10));

            info!(
                "Runtime shutdown complete for node {} ({})",
                node_name, node_id
            );
        }

        self.set_status(NodeStatus::Stopped);
        Ok(())
    }
}

/// Manages multiple node instances
#[derive(Clone)]
pub struct NodeManager {
    nodes: Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
    message_sender: mpsc::Sender<TuiMessage>,
    governance: Arc<MockGovernance>,
    run_id: String,
}

impl NodeManager {
    /// Create a new node manager
    #[must_use]
    pub fn new(message_sender: mpsc::Sender<TuiMessage>, governance: Arc<MockGovernance>) -> Self {
        let run_id = Uuid::new_v4().to_string();
        info!("Created new NodeManager with run_id: {}", run_id);

        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            governance,
            run_id,
        }
    }

    /// Start the command handler
    pub fn start_command_handler(&self, command_receiver: mpsc::Receiver<NodeCommand>) {
        let nodes = self.nodes.clone();
        let message_sender = self.message_sender.clone();
        let governance = self.governance.clone();
        let run_id = self.run_id.clone();

        thread::spawn(move || {
            while let Ok(command) = command_receiver.recv() {
                match command {
                    NodeCommand::StartNode { id, name, config } => {
                        Self::handle_start_node(
                            &nodes,
                            &message_sender,
                            &governance,
                            &run_id,
                            id,
                            name,
                            config,
                        );
                    }

                    NodeCommand::StopNode { id } => {
                        Self::handle_stop_node(&nodes, message_sender.clone(), &governance, id);
                    }

                    NodeCommand::RestartNode { id } => {
                        Self::handle_restart_node(
                            nodes.clone(),
                            message_sender.clone(),
                            &governance,
                            &run_id,
                            id,
                        );
                    }

                    NodeCommand::GetStatus => {
                        Self::handle_get_status(&nodes, &message_sender);
                    }

                    NodeCommand::Shutdown => {
                        Self::handle_shutdown(nodes.clone(), message_sender);
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
        run_id: &str,
        id: NodeId,
        name: String,
        config: Option<Box<TuiNodeConfig>>,
    ) {
        info!("Starting node {name} ({id})");

        // Create or use provided configuration
        let node_config = config.map_or_else(
            || Self::create_node_config(id, &name, governance, run_id),
            |config| *config,
        );

        // Create the node handle
        let mut node_handle = NodeHandle::new(id, name.clone(), node_config.clone());

        // Create a dedicated tokio runtime for this node
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(format!("node-{}-{}", id.execution_order(), id.pokemon_id()))
            .thread_stack_size(2 * 1024 * 1024) // 2MB stack
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(e) => {
                error!("Failed to create runtime for node {name} ({id}): {e}");
                let _ = message_sender.send(TuiMessage::NodeFailed {
                    id,
                    error: format!("Failed to create runtime: {e}"),
                });
                return;
            }
        };

        // Store the runtime in the handle
        node_handle.runtime = Some(runtime);

        // Get a reference to the runtime for spawning the task
        let runtime_ref = node_handle.runtime.as_ref().unwrap();

        // Clone necessary data for the task
        let task_message_sender = message_sender.clone();
        let task_governance = governance.clone();
        let shutdown_token = node_handle.shutdown_token.clone();

        // Add the node to governance within the runtime
        let public_key = node_config.node_key.verifying_key();

        // Determine the proper origin endpoint based on whether this is the first node
        // Use the actual allocated port from the node configuration
        let actual_port = node_config.port;
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

        // Clone name for logging after the spawn
        let node_name_for_log = name.clone();

        // Spawn the node task within its dedicated runtime
        let task_handle = runtime_ref.spawn(async move {
            // Add to governance
            if let Err(e) = task_governance.add_node(topology_node) {
                error!("Failed to add node {name} ({id}) to governance: {e}");
                let _ = task_message_sender.send(TuiMessage::NodeFailed {
                    id,
                    error: format!("Failed to add to governance: {e}"),
                });
                return Err(proven_local::Error::Governance(format!(
                    "Failed to add to governance: {e}"
                )));
            }

            // Notify that the node has started
            let _ = task_message_sender.send(TuiMessage::NodeStarted {
                id,
                name: name.clone(),
                config: Box::new(node_config.clone()),
            });

            // Run the node - it will handle the shutdown token internally
            let result = proven_local::run_node(node_config, shutdown_token.clone()).await;
            match result {
                Ok(()) => {
                    info!("Node {name} ({id}) completed successfully");
                    let _ = task_message_sender.send(TuiMessage::NodeStopped { id });
                    Ok(())
                }
                Err(e) => {
                    error!("Node {name} ({id}) failed: {e}");
                    let _ = task_message_sender.send(TuiMessage::NodeFailed {
                        id,
                        error: e.to_string(),
                    });
                    Err(e)
                }
            }
        });

        // Store the task handle
        node_handle.task_handle = Some(task_handle);
        node_handle.set_status(NodeStatus::Running);

        // Add to the nodes map
        nodes.write().insert(id, node_handle);

        info!("Node {node_name_for_log} ({id}) started with dedicated runtime");
    }

    fn handle_stop_node(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
        id: NodeId,
    ) {
        // Get the node handle from the map
        let mut handle = match nodes.write().remove(&id) {
            Some(handle) => handle,
            None => {
                warn!("Attempted to stop non-existent node {id}");
                return;
            }
        };

        let governance = governance.clone();

        // Spawn a thread to handle the async operations
        thread::spawn(move || {
            // Remove from governance (if this fails, just log)
            let public_key = hex::encode(handle.config.node_key.verifying_key().to_bytes());
            if let Err(e) = governance.remove_node(&public_key) {
                warn!("Failed to remove node {id} from governance: {e}");
            }

            // Stop the node
            if let Err(e) = handle.stop() {
                error!("Failed to stop node {id}: {e}");
                let _ = message_sender.send(TuiMessage::NodeFailed {
                    id,
                    error: format!("Failed to stop: {e}"),
                });
            } else {
                info!("Node {id} stopped successfully");
                let _ = message_sender.send(TuiMessage::NodeStopped { id });
            }
        });
    }

    fn handle_restart_node(
        nodes: Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: mpsc::Sender<TuiMessage>,
        governance: &Arc<MockGovernance>,
        run_id: &str,
        id: NodeId,
    ) {
        // First stop the node
        Self::handle_stop_node(&nodes, message_sender.clone(), governance, id);

        // Wait a moment for the stop to complete
        thread::sleep(Duration::from_millis(500));

        // Generate a new name and start the node again
        let name = id.display_name();
        Self::handle_start_node(&nodes, &message_sender, governance, run_id, id, name, None);
    }

    fn handle_get_status(
        nodes: &Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: &mpsc::Sender<TuiMessage>,
    ) {
        let nodes_read = nodes.read();
        for handle in nodes_read.values() {
            let _ = message_sender.send(TuiMessage::NodeStatusUpdate {
                id: handle.id,
                status: handle.status.clone(),
            });
        }
    }

    fn handle_shutdown(
        nodes: Arc<RwLock<HashMap<NodeId, NodeHandle>>>,
        message_sender: mpsc::Sender<TuiMessage>,
    ) {
        info!("Shutting down all nodes");

        // Get all handles
        let handles: Vec<NodeHandle> = {
            let mut nodes_write = nodes.write();
            nodes_write.drain().map(|(_, handle)| handle).collect()
        };

        // Stop all nodes in parallel using threads
        let mut join_handles = Vec::new();
        for mut handle in handles {
            let handle_name = handle.name.clone();
            let handle_id = handle.id;

            let join_handle = thread::spawn(move || {
                info!("Stopping node {} ({})", handle_name, handle_id);
                if let Err(e) = handle.stop() {
                    error!("Failed to stop node {}: {e}", handle_id);
                } else {
                    info!("Node {} ({}) stopped successfully", handle_name, handle_id);
                }
            });
            join_handles.push(join_handle);
        }

        // Wait for all shutdowns to complete
        for join_handle in join_handles {
            let _ = join_handle.join();
        }

        info!("All nodes shut down");
        let _ = message_sender.send(TuiMessage::ShutdownComplete);
    }

    /// Get the current status of all nodes
    #[must_use]
    pub fn get_all_nodes(&self) -> HashMap<NodeId, (String, NodeStatus)> {
        let nodes_read = self.nodes.read();
        nodes_read
            .iter()
            .map(|(id, handle)| (*id, (handle.name.clone(), handle.status.clone())))
            .collect()
    }

    /// Create a node configuration and add the node to governance
    fn create_node_config(
        id: NodeId,
        name: &str,
        governance: &Arc<MockGovernance>,
        run_id: &str,
    ) -> proven_local::NodeConfig<proven_governance_mock::MockGovernance> {
        // Generate a random private key for this node
        let private_key = SigningKey::generate(&mut rand::thread_rng());

        // Get the main port (first port allocated will be 3000 for the first node)
        let main_port = allocate_port().unwrap_or_else(|e| {
            panic!("Failed to allocate main port for node {}: {}", id, e);
        });

        info!(
            "Allocated main port for node {} ({}): {}",
            name, id, main_port
        );

        // Note: Governance registration is handled in handle_start_node when the node actually starts
        Self::build_node_config(name, main_port, governance, private_key, run_id)
    }

    /// Build the node configuration with all service ports and directories
    #[allow(clippy::too_many_lines)]
    pub fn build_node_config(
        name: &str,
        main_port: u16,
        governance: &Arc<MockGovernance>,
        private_key: SigningKey,
        run_id: &str,
    ) -> TuiNodeConfig {
        TuiNodeConfig {
            allow_single_node: false,
            bitcoin_mainnet_fallback_rpc_endpoint: Url::parse("https://bitcoin-rpc.publicnode.com")
                .unwrap(),
            bitcoin_mainnet_proxy_port: allocate_port().unwrap(),
            bitcoin_mainnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/bitcoin-mainnet"
            )),

            bitcoin_testnet_fallback_rpc_endpoint: Url::parse(
                "https://bitcoin-testnet-rpc.publicnode.com",
            )
            .unwrap(),
            bitcoin_testnet_proxy_port: allocate_port().unwrap(),
            bitcoin_testnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/bitcoin-testnet"
            )),

            ethereum_holesky_consensus_http_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_holesky_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-holesky/lighthouse"
            )),

            ethereum_holesky_execution_discovery_port: allocate_port().unwrap(),
            ethereum_holesky_execution_http_port: allocate_port().unwrap(),
            ethereum_holesky_execution_metrics_port: allocate_port().unwrap(),
            ethereum_holesky_execution_rpc_port: allocate_port().unwrap(),
            ethereum_holesky_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-holesky/reth"
            )),
            ethereum_holesky_fallback_rpc_endpoint: Url::parse(
                "https://ethereum-holesky-rpc.publicnode.com",
            )
            .unwrap(),

            ethereum_mainnet_consensus_http_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_mainnet_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-mainnet/lighthouse"
            )),

            ethereum_mainnet_execution_discovery_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_http_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_metrics_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_rpc_port: allocate_port().unwrap(),
            ethereum_mainnet_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-mainnet/reth"
            )),
            ethereum_mainnet_fallback_rpc_endpoint: Url::parse(
                "https://ethereum-rpc.publicnode.com",
            )
            .unwrap(),

            ethereum_sepolia_consensus_http_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_metrics_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_p2p_port: allocate_port().unwrap(),
            ethereum_sepolia_consensus_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-sepolia/lighthouse"
            )),

            ethereum_sepolia_execution_discovery_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_http_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_metrics_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_rpc_port: allocate_port().unwrap(),
            ethereum_sepolia_execution_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/ethereum-sepolia/reth"
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
            nats_config_dir: PathBuf::from(format!("/tmp/proven/{run_id}/{name}/nats-config")),
            nats_http_port: allocate_port().unwrap(),
            nats_store_dir: PathBuf::from(format!("/tmp/proven/{run_id}/{name}/nats")),

            network_config_path: None, // Using shared governance instead
            node_key: private_key,

            postgres_bin_path: PathBuf::from("/usr/local/pgsql/bin"),
            postgres_port: allocate_port().unwrap(),
            postgres_skip_vacuum: false,
            postgres_store_dir: PathBuf::from(format!("/tmp/proven/{run_id}/{name}/postgres")),

            radix_mainnet_fallback_rpc_endpoint: Url::parse("https://mainnet.radixdlt.com")
                .unwrap(),
            radix_mainnet_http_port: allocate_port().unwrap(),
            radix_mainnet_p2p_port: allocate_port().unwrap(),
            radix_mainnet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/radix-node-mainnet"
            )),

            radix_stokenet_fallback_rpc_endpoint: Url::parse("https://stokenet.radixdlt.com")
                .unwrap(),
            radix_stokenet_http_port: allocate_port().unwrap(),
            radix_stokenet_p2p_port: allocate_port().unwrap(),
            radix_stokenet_store_dir: PathBuf::from(format!(
                "/tmp/proven/{run_id}/{name}/radix-node-stokenet"
            )),
        }
    }
}
