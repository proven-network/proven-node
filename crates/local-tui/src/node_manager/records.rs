//! Node record types and handles for managing node lifecycle

use crate::ip_allocator::allocate_port;
use crate::messages::{NodeOperation, TuiNodeConfig};
use crate::node_id::NodeId;

use anyhow::Result;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use proven_governance::TopologyNode;
use proven_governance_mock::MockGovernance;
use proven_local::{Node, NodeStatus};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use std::{fs, os::unix::fs::symlink};
use tracing::{error, info, warn};
use url::Url;

/// Message sent from `NodeHandle` back to `NodeManager` when a node finishes
#[derive(Debug)]
pub enum NodeManagerMessage {
    NodeFinished {
        id: NodeId,
        name: String,
        final_status: NodeStatus,
        specializations: HashSet<proven_governance::NodeSpecialization>,
    },
}

/// Record of a node in the manager - either currently running or stopped
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // TODO: Box NodeHandle
pub enum NodeRecord {
    Running(NodeHandle),
    Stopped {
        _id: NodeId,
        name: String,
        final_status: NodeStatus,
        specializations: HashSet<proven_governance::NodeSpecialization>,
        _stopped_at: std::time::Instant,
    },
}

/// Handle to a running node with its metadata and command processing
#[derive(Debug)]
pub struct NodeHandle {
    /// Node identifier
    pub _id: NodeId,

    /// Display name (publicly accessible for TUI display)
    pub name: String,

    /// Node configuration (stored for URL extraction)
    pub config: TuiNodeConfig,

    /// Specializations for this node
    pub specializations: HashSet<proven_governance::NodeSpecialization>,

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
        specializations: HashSet<proven_governance::NodeSpecialization>,
        governance: Arc<MockGovernance>,
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
        id: NodeId,
        name: &str,
        config: &TuiNodeConfig,
        command_receiver: &mpsc::Receiver<NodeOperation>,
        governance: &Arc<MockGovernance>,
        status: &Arc<RwLock<NodeStatus>>,
        completion_sender: &mpsc::Sender<NodeManagerMessage>,
        specializations: HashSet<proven_governance::NodeSpecialization>,
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
                    if let Some(mut new_config) = new_config {
                        // Create persistent symlinks and update config if we have specializations
                        if let Some(ref node_specializations) = specializations {
                            if !node_specializations.is_empty() {
                                if let Err(e) = create_persistent_symlinks(
                                    node_specializations,
                                    &mut new_config,
                                ) {
                                    error!(
                                        "Failed to create persistent symlinks for node {} ({}): {}",
                                        name, id, e
                                    );
                                    // Continue without persistent storage if symlinks fail
                                }
                            }
                        }
                        node = Node::new(*new_config);
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
        specializations: Option<HashSet<proven_governance::NodeSpecialization>>,
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

        let topology_node = TopologyNode {
            availability_zone: "local".to_string(),
            origin,
            public_key: hex::encode(public_key.to_bytes()),
            region: "local".to_string(),
            specializations: node_specializations,
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

        // Sync status with actual node after start operation to ensure UI reflects true state
        Self::sync_node_status(runtime, node, status);
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

/// Create a default node configuration
pub fn create_node_config(
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

    build_node_config(name, main_port, governance, private_key, session_id)
}

/// Create persistent symlinks for specialized nodes
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_lines)]
fn create_persistent_symlinks(
    specializations: &HashSet<proven_governance::NodeSpecialization>,
    node_config: &mut TuiNodeConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    if specializations.is_empty() {
        return Ok(());
    }

    // Get home directory and create ~/.proven if it doesn't exist
    let home_dir = dirs::home_dir().ok_or("Could not determine home directory")?;
    let proven_dir = home_dir.join(".proven");
    fs::create_dir_all(&proven_dir)?;

    info!(
        "Creating persistent symlinks in {:?} for specializations: {:?}",
        proven_dir, specializations
    );

    for specialization in specializations {
        match specialization {
            proven_governance::NodeSpecialization::BitcoinMainnet => {
                let persistent_dir = find_next_available_dir(&proven_dir, "bitcoin-mainnet")?;
                create_symlink_and_update_config(
                    &persistent_dir,
                    &node_config.bitcoin_mainnet_store_dir,
                )?;
                node_config.bitcoin_mainnet_store_dir = persistent_dir;
                info!(
                    "Bitcoin Mainnet data will persist at: {:?}",
                    node_config.bitcoin_mainnet_store_dir
                );
            }
            proven_governance::NodeSpecialization::BitcoinTestnet => {
                let persistent_dir = find_next_available_dir(&proven_dir, "bitcoin-testnet")?;
                create_symlink_and_update_config(
                    &persistent_dir,
                    &node_config.bitcoin_testnet_store_dir,
                )?;
                node_config.bitcoin_testnet_store_dir = persistent_dir;
                info!(
                    "Bitcoin Testnet data will persist at: {:?}",
                    node_config.bitcoin_testnet_store_dir
                );
            }
            proven_governance::NodeSpecialization::EthereumMainnet => {
                // Handle both consensus and execution
                let consensus_dir =
                    find_next_available_dir(&proven_dir, "ethereum-mainnet-consensus")?;
                let execution_dir =
                    find_next_available_dir(&proven_dir, "ethereum-mainnet-execution")?;

                create_symlink_and_update_config(
                    &consensus_dir,
                    &node_config.ethereum_mainnet_consensus_store_dir,
                )?;
                create_symlink_and_update_config(
                    &execution_dir,
                    &node_config.ethereum_mainnet_execution_store_dir,
                )?;

                node_config.ethereum_mainnet_consensus_store_dir = consensus_dir;
                node_config.ethereum_mainnet_execution_store_dir = execution_dir;
                info!(
                    "Ethereum Mainnet data will persist at: {:?} (consensus), {:?} (execution)",
                    node_config.ethereum_mainnet_consensus_store_dir,
                    node_config.ethereum_mainnet_execution_store_dir
                );
            }
            proven_governance::NodeSpecialization::EthereumHolesky => {
                // Handle both consensus and execution
                let consensus_dir =
                    find_next_available_dir(&proven_dir, "ethereum-holesky-consensus")?;
                let execution_dir =
                    find_next_available_dir(&proven_dir, "ethereum-holesky-execution")?;

                create_symlink_and_update_config(
                    &consensus_dir,
                    &node_config.ethereum_holesky_consensus_store_dir,
                )?;
                create_symlink_and_update_config(
                    &execution_dir,
                    &node_config.ethereum_holesky_execution_store_dir,
                )?;

                node_config.ethereum_holesky_consensus_store_dir = consensus_dir;
                node_config.ethereum_holesky_execution_store_dir = execution_dir;
                info!(
                    "Ethereum Holesky data will persist at: {:?} (consensus), {:?} (execution)",
                    node_config.ethereum_holesky_consensus_store_dir,
                    node_config.ethereum_holesky_execution_store_dir
                );
            }
            proven_governance::NodeSpecialization::EthereumSepolia => {
                // Handle both consensus and execution
                let consensus_dir =
                    find_next_available_dir(&proven_dir, "ethereum-sepolia-consensus")?;
                let execution_dir =
                    find_next_available_dir(&proven_dir, "ethereum-sepolia-execution")?;

                create_symlink_and_update_config(
                    &consensus_dir,
                    &node_config.ethereum_sepolia_consensus_store_dir,
                )?;
                create_symlink_and_update_config(
                    &execution_dir,
                    &node_config.ethereum_sepolia_execution_store_dir,
                )?;

                node_config.ethereum_sepolia_consensus_store_dir = consensus_dir;
                node_config.ethereum_sepolia_execution_store_dir = execution_dir;
                info!(
                    "Ethereum Sepolia data will persist at: {:?} (consensus), {:?} (execution)",
                    node_config.ethereum_sepolia_consensus_store_dir,
                    node_config.ethereum_sepolia_execution_store_dir
                );
            }
            proven_governance::NodeSpecialization::RadixMainnet => {
                let persistent_dir = find_next_available_dir(&proven_dir, "radix-mainnet")?;
                create_symlink_and_update_config(
                    &persistent_dir,
                    &node_config.radix_mainnet_store_dir,
                )?;
                node_config.radix_mainnet_store_dir = persistent_dir;
                info!(
                    "Radix Mainnet data will persist at: {:?}",
                    node_config.radix_mainnet_store_dir
                );
            }
            proven_governance::NodeSpecialization::RadixStokenet => {
                let persistent_dir = find_next_available_dir(&proven_dir, "radix-stokenet")?;
                create_symlink_and_update_config(
                    &persistent_dir,
                    &node_config.radix_stokenet_store_dir,
                )?;
                node_config.radix_stokenet_store_dir = persistent_dir;
                info!(
                    "Radix Stokenet data will persist at: {:?}",
                    node_config.radix_stokenet_store_dir
                );
            }
        }
    }

    Ok(())
}

/// Find the next available directory name with counter (e.g., bitcoin-mainnet-1, bitcoin-mainnet-2, etc.)
fn find_next_available_dir(
    base_dir: &Path,
    prefix: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    for i in 1..=999 {
        let dir_name = format!("{prefix}-{i}");
        let dir_path = base_dir.join(&dir_name);
        if !dir_path.exists() {
            return Ok(dir_path);
        }
    }
    Err(format!("Could not find available directory name for prefix: {prefix}").into())
}

/// Create a symlink from persistent directory to session directory
fn create_symlink_and_update_config(
    persistent_dir: &PathBuf,
    session_dir: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create the session directory first
    fs::create_dir_all(session_dir)?;

    // Create the symlink from persistent location to session location
    if let Err(e) = symlink(session_dir, persistent_dir) {
        warn!(
            "Failed to create symlink from {:?} to {:?}: {}",
            persistent_dir, session_dir, e
        );
        // If symlink fails, we can still continue - just won't have persistence
    } else {
        info!("Created symlink: {:?} -> {:?}", persistent_dir, session_dir);
    }

    Ok(())
}

/// Build a complete node configuration
#[allow(clippy::too_many_lines)]
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

        nats_cli_bin_dir: None,
        nats_client_port: allocate_port().unwrap(),
        nats_cluster_port: allocate_port().unwrap(),
        nats_config_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/nats-config")),
        nats_http_port: allocate_port().unwrap(),
        nats_server_bin_dir: None,
        nats_store_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/nats")),

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
