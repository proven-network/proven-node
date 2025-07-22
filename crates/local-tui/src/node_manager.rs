//! Node lifecycle management with clean interface

pub mod records;

use crate::messages::NodeOperation;
use crate::node_id::TuiNodeId;
use records::{NodeHandle, NodeManagerMessage, NodeRecord};

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::symlink;
use std::sync::{Arc, mpsc};

use parking_lot::RwLock;
use proven_local::{NodeConfig, NodeStatus};
use proven_logger::{error, info, warn};
use proven_topology_mock::MockTopologyAdaptor;

// Type aliases to reduce complexity
type NodeMap = Arc<RwLock<HashMap<TuiNodeId, NodeRecord>>>;
type CompletionReceiver = Arc<std::sync::Mutex<mpsc::Receiver<NodeManagerMessage>>>;
type PersistentDirMap = Arc<RwLock<HashMap<String, HashSet<u32>>>>;
type NodeDirAssignments = Arc<RwLock<HashMap<TuiNodeId, Vec<(String, u32)>>>>;
type NodeUiInfo = HashMap<
    TuiNodeId,
    (
        String,
        NodeStatus,
        HashSet<proven_topology::NodeSpecialization>,
    ),
>;

/// Manages multiple node instances with a clean interface
#[derive(Clone, Debug)]
pub struct NodeManager {
    nodes: NodeMap,
    governance: Arc<MockTopologyAdaptor>,
    session_id: String,
    completion_sender: mpsc::Sender<NodeManagerMessage>,
    completion_receiver: CompletionReceiver,
    /// Track which persistent directories are currently in use by this TUI session
    /// Maps specialization name (like "bitcoin-testnet") to set of directory numbers in use
    used_persistent_dirs: PersistentDirMap,
    /// Track which directories each node is using for cleanup purposes
    node_dir_assignments: NodeDirAssignments,
}

impl NodeManager {
    /// Create a new node manager
    #[must_use]
    pub fn new(governance: Arc<MockTopologyAdaptor>, session_id: String) -> Self {
        info!("Created new NodeManager with session_id: {session_id}");

        let (completion_sender, completion_receiver) = mpsc::channel();

        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            governance,
            session_id,
            completion_sender,
            completion_receiver: Arc::new(std::sync::Mutex::new(completion_receiver)),
            used_persistent_dirs: Arc::new(RwLock::new(HashMap::new())),
            node_dir_assignments: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a new node with optional configuration
    #[allow(clippy::cognitive_complexity)]
    /// Start a new node with specific specializations
    #[allow(clippy::cognitive_complexity)]
    pub fn start_node(
        &self,
        id: TuiNodeId,
        name: &str,
        specializations: HashSet<proven_topology::NodeSpecialization>,
    ) {
        info!(
            "Starting node {} ({}) with specializations: {:?}",
            name,
            id.full_pokemon_name(),
            specializations
        );

        let node_config = self.create_node_config_with_tracking(id, name, &specializations);

        // Check if node already exists and handle accordingly
        {
            let nodes_guard = self.nodes.read();
            if let Some(existing_record) = nodes_guard.get(&id) {
                match existing_record {
                    NodeRecord::Running(handle) => {
                        // Node is running, send start command to it
                        if let Err(e) = handle.send_command(NodeOperation::Start {
                            config: Some(Box::new(node_config)),
                            specializations: Some(specializations),
                        }) {
                            error!(
                                "Failed to send start command to running node {name} ({id}): {e}"
                            );
                        }
                        return;
                    }
                    NodeRecord::Stopped { .. } => {
                        // Node was stopped, we'll replace it with a new running instance
                        info!("Restarting previously stopped node {name}");
                    }
                }
            }
        }

        // Create new node handle with command processing
        let node_handle = NodeHandle::new(
            id,
            name.to_string(),
            node_config.clone(),
            specializations.clone(),
            self.governance.clone(),
            self.completion_sender.clone(),
        );

        // Add to nodes map as Running
        {
            let mut nodes_guard = self.nodes.write();
            nodes_guard.insert(id, NodeRecord::Running(node_handle));
        }

        // Send start command to the newly created node
        {
            let nodes_guard = self.nodes.read();
            if let Some(NodeRecord::Running(node_handle)) = nodes_guard.get(&id)
                && let Err(e) = node_handle.send_command(NodeOperation::Start {
                    config: Some(Box::new(node_config)),
                    specializations: Some(specializations),
                })
            {
                error!("Failed to send start command to node {name} ({id}): {e}");
            }
        }
    }

    /// Stop a node
    #[allow(clippy::cognitive_complexity)]
    pub fn stop_node(&self, id: TuiNodeId) {
        let nodes_guard = self.nodes.read();
        if let Some(record) = nodes_guard.get(&id) {
            match record {
                NodeRecord::Running(handle) => {
                    info!("Stopping node {}", id.full_pokemon_name());
                    if let Err(e) = handle.send_command(NodeOperation::Stop) {
                        error!("Failed to send stop command to node {id}: {e}");
                    }
                }
                NodeRecord::Stopped { .. } => {
                    info!("Node {} is already stopped", id.full_pokemon_name());
                }
            }
        } else {
            warn!("Attempted to stop non-existent node: {id}");
        }
    }

    /// Restart a node
    #[allow(clippy::cognitive_complexity)]
    pub fn restart_node(&self, id: TuiNodeId) {
        let nodes_guard = self.nodes.read();
        if let Some(record) = nodes_guard.get(&id) {
            match record {
                NodeRecord::Running(handle) => {
                    info!("Restarting node {}", id.full_pokemon_name());
                    if let Err(e) = handle.send_command(NodeOperation::Restart) {
                        error!("Failed to send restart command to node {id}: {e}");
                    }
                }
                NodeRecord::Stopped { .. } => {
                    info!(
                        "Cannot restart stopped node {} - use start instead",
                        id.full_pokemon_name()
                    );
                }
            }
        } else {
            warn!("Attempted to restart non-existent node: {id}");
        }
    }

    /// Shutdown all nodes
    #[allow(clippy::cognitive_complexity)]
    pub fn shutdown_all(&self) {
        info!("Shutting down all nodes...");

        // Send shutdown commands to all running nodes (non-blocking!)
        // Leave nodes in HashMap so caller can monitor their status
        {
            let nodes_guard = self.nodes.read();
            for (id, record) in nodes_guard.iter() {
                match record {
                    NodeRecord::Running(handle) => {
                        info!(
                            "Sending shutdown command to node {}",
                            id.full_pokemon_name()
                        );
                        if let Err(e) = handle.send_command(NodeOperation::Shutdown) {
                            error!("Failed to send shutdown command to node {id}: {e}");
                        }
                    }
                    NodeRecord::Stopped { .. } => {
                        info!("Node {} already stopped", id.full_pokemon_name());
                    }
                }
            }
        }

        info!("Shutdown commands sent to all running nodes - waiting for graceful shutdown");
    }

    /// Get or allocate a persistent directory for a specialization
    /// This reuses existing directories that aren't currently in use by this session
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` is poisoned
    #[must_use]
    pub fn get_persistent_directory(&self, specialization_prefix: &str) -> Option<u32> {
        let mut used_dirs = self.used_persistent_dirs.write();

        // Get the set of currently used directory numbers for this specialization
        let used_set = used_dirs
            .entry(specialization_prefix.to_string())
            .or_default();

        // Find existing directories on disk
        let home_dir = dirs::home_dir()?;
        let proven_dir = home_dir.join(".proven");

        if !proven_dir.exists() {
            // If .proven doesn't exist, start with directory 1
            used_set.insert(1);
            return Some(1);
        }

        // Find all existing directories with this prefix
        let mut existing_dirs = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&proven_dir) {
            for entry in entries.flatten() {
                if let Some(dir_name) = entry.file_name().to_str()
                    && dir_name.starts_with(&format!("{specialization_prefix}-"))
                    && let Some(number_part) =
                        dir_name.strip_prefix(&format!("{specialization_prefix}-"))
                    && let Ok(num) = number_part.parse::<u32>()
                {
                    existing_dirs.push(num);
                }
            }
        }

        existing_dirs.sort_unstable();

        // Try to reuse an existing directory that's not currently in use
        for &dir_num in &existing_dirs {
            if used_set.insert(dir_num) {
                return Some(dir_num);
            }
        }

        // All existing directories are in use, find the next available number
        let next_num = if existing_dirs.is_empty() {
            1
        } else {
            existing_dirs.iter().max().unwrap() + 1
        };

        used_set.insert(next_num);
        drop(used_dirs);

        Some(next_num)
    }

    /// Release a persistent directory when a node is stopped
    pub fn release_persistent_directory(&self, specialization_prefix: &str, dir_num: u32) {
        let mut used_dirs = self.used_persistent_dirs.write();
        if let Some(used_set) = used_dirs.get_mut(specialization_prefix) {
            used_set.remove(&dir_num);
        }
    }

    /// Create node config with persistent directory tracking
    fn create_node_config_with_tracking(
        &self,
        id: TuiNodeId,
        name: &str,
        specializations: &HashSet<proven_topology::NodeSpecialization>,
    ) -> NodeConfig<MockTopologyAdaptor> {
        // Create base config
        let node_config = records::create_node_config(id, name, &self.governance, &self.session_id);

        // Handle persistent directories for specializations
        if !specializations.is_empty()
            && let Err(e) =
                self.create_persistent_symlinks_with_tracking(id, specializations, &node_config)
        {
            error!(
                "Failed to create persistent symlinks for node {name} ({id}): {e}. Continuing with temporary storage."
            );
        }

        node_config
    }

    /// Create persistent symlinks with session tracking
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_lines)]
    fn create_persistent_symlinks_with_tracking(
        &self,
        node_id: TuiNodeId,
        specializations: &HashSet<proven_topology::NodeSpecialization>,
        node_config: &NodeConfig<MockTopologyAdaptor>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        // Get home directory and create ~/.proven if it doesn't exist
        let home_dir = dirs::home_dir().ok_or("Could not determine home directory")?;
        let proven_dir = home_dir.join(".proven");
        fs::create_dir_all(&proven_dir)?;

        info!(
            "Creating persistent symlinks in {} for specializations: {specializations:?}",
            proven_dir.display()
        );

        // Track directory assignments for this node
        let mut assignments = Vec::new();

        for specialization in specializations {
            match specialization {
                proven_topology::NodeSpecialization::BitcoinMainnet => {
                    if let Some(dir_num) = self.get_persistent_directory("bitcoin-mainnet") {
                        let persistent_dir = proven_dir.join(format!("bitcoin-mainnet-{dir_num}"));
                        Self::create_symlink_and_update_config(
                            &persistent_dir,
                            &node_config.bitcoin_mainnet_store_dir,
                        )?;
                        assignments.push(("bitcoin-mainnet".to_string(), dir_num));
                        info!(
                            "Bitcoin Mainnet data will persist at: {} (via symlink from {})",
                            persistent_dir.display(),
                            node_config.bitcoin_mainnet_store_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::BitcoinTestnet => {
                    if let Some(dir_num) = self.get_persistent_directory("bitcoin-testnet") {
                        let persistent_dir = proven_dir.join(format!("bitcoin-testnet-{dir_num}"));
                        Self::create_symlink_and_update_config(
                            &persistent_dir,
                            &node_config.bitcoin_testnet_store_dir,
                        )?;
                        assignments.push(("bitcoin-testnet".to_string(), dir_num));
                        info!(
                            "Bitcoin Testnet data will persist at: {} (via symlink from {})",
                            persistent_dir.display(),
                            node_config.bitcoin_testnet_store_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::EthereumMainnet => {
                    if let Some(dir_num) = self.get_persistent_directory("ethereum-mainnet") {
                        let base_persistent_dir =
                            proven_dir.join(format!("ethereum-mainnet-{dir_num}"));
                        let consensus_persistent_dir = base_persistent_dir.join("lighthouse");
                        let execution_persistent_dir = base_persistent_dir.join("reth");

                        Self::create_symlink_and_update_config(
                            &consensus_persistent_dir,
                            &node_config.ethereum_mainnet_consensus_store_dir,
                        )?;
                        Self::create_symlink_and_update_config(
                            &execution_persistent_dir,
                            &node_config.ethereum_mainnet_execution_store_dir,
                        )?;
                        assignments.push(("ethereum-mainnet".to_string(), dir_num));
                        info!(
                            "Ethereum Mainnet data will persist at: {} (consensus: {}, execution: {})",
                            base_persistent_dir.display(),
                            consensus_persistent_dir.display(),
                            execution_persistent_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::EthereumHolesky => {
                    if let Some(dir_num) = self.get_persistent_directory("ethereum-holesky") {
                        let base_persistent_dir =
                            proven_dir.join(format!("ethereum-holesky-{dir_num}"));
                        let consensus_persistent_dir = base_persistent_dir.join("lighthouse");
                        let execution_persistent_dir = base_persistent_dir.join("reth");

                        Self::create_symlink_and_update_config(
                            &consensus_persistent_dir,
                            &node_config.ethereum_holesky_consensus_store_dir,
                        )?;
                        Self::create_symlink_and_update_config(
                            &execution_persistent_dir,
                            &node_config.ethereum_holesky_execution_store_dir,
                        )?;
                        assignments.push(("ethereum-holesky".to_string(), dir_num));
                        info!(
                            "Ethereum Holesky data will persist at: {} (consensus: {}, execution: {})",
                            base_persistent_dir.display(),
                            consensus_persistent_dir.display(),
                            execution_persistent_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::EthereumSepolia => {
                    if let Some(dir_num) = self.get_persistent_directory("ethereum-sepolia") {
                        let base_persistent_dir =
                            proven_dir.join(format!("ethereum-sepolia-{dir_num}"));
                        let consensus_persistent_dir = base_persistent_dir.join("lighthouse");
                        let execution_persistent_dir = base_persistent_dir.join("reth");

                        Self::create_symlink_and_update_config(
                            &consensus_persistent_dir,
                            &node_config.ethereum_sepolia_consensus_store_dir,
                        )?;
                        Self::create_symlink_and_update_config(
                            &execution_persistent_dir,
                            &node_config.ethereum_sepolia_execution_store_dir,
                        )?;
                        assignments.push(("ethereum-sepolia".to_string(), dir_num));
                        info!(
                            "Ethereum Sepolia data will persist at: {} (consensus: {}, execution: {})",
                            base_persistent_dir.display(),
                            consensus_persistent_dir.display(),
                            execution_persistent_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::RadixMainnet => {
                    if let Some(dir_num) = self.get_persistent_directory("radix-mainnet") {
                        let persistent_dir = proven_dir.join(format!("radix-mainnet-{dir_num}"));
                        Self::create_symlink_and_update_config(
                            &persistent_dir,
                            &node_config.radix_mainnet_store_dir,
                        )?;
                        assignments.push(("radix-mainnet".to_string(), dir_num));
                        info!(
                            "Radix Mainnet data will persist at: {} (via symlink from {})",
                            persistent_dir.display(),
                            node_config.radix_mainnet_store_dir.display()
                        );
                    }
                }
                proven_topology::NodeSpecialization::RadixStokenet => {
                    if let Some(dir_num) = self.get_persistent_directory("radix-stokenet") {
                        let persistent_dir = proven_dir.join(format!("radix-stokenet-{dir_num}"));
                        Self::create_symlink_and_update_config(
                            &persistent_dir,
                            &node_config.radix_stokenet_store_dir,
                        )?;
                        assignments.push(("radix-stokenet".to_string(), dir_num));
                        info!(
                            "Radix Stokenet data will persist at: {} (via symlink from {})",
                            persistent_dir.display(),
                            node_config.radix_stokenet_store_dir.display()
                        );
                    }
                }
            }
        }

        // Always handle RocksDB storage directory regardless of specializations
        // This ensures engine persistence across restarts
        if let Some(dir_num) = self.get_persistent_directory("rocksdb") {
            let persistent_dir = proven_dir.join(format!("rocksdb-{dir_num}"));
            Self::create_symlink_and_update_config(
                &persistent_dir,
                &node_config.rocksdb_store_dir,
            )?;
            assignments.push(("rocksdb".to_string(), dir_num));
            info!(
                "RocksDB data will persist at: {} (via symlink from {})",
                persistent_dir.display(),
                node_config.rocksdb_store_dir.display()
            );
        }

        // Store directory assignments for this node
        if !assignments.is_empty() {
            let mut node_assignments = self.node_dir_assignments.write();
            node_assignments.insert(node_id, assignments);
        }

        Ok(())
    }

    /// Create a symlink from session directory to persistent directory
    fn create_symlink_and_update_config(
        persistent_dir: &std::path::PathBuf,
        session_dir: &std::path::PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        // Create the persistent directory first (this is where real data will live)
        fs::create_dir_all(persistent_dir)?;

        // Create parent directory for session path if it doesn't exist
        if let Some(parent) = session_dir.parent() {
            fs::create_dir_all(parent)?;
        }

        // Remove session directory if it exists (so we can create symlink)
        if session_dir.exists() {
            if session_dir.is_dir() && !session_dir.is_symlink() {
                fs::remove_dir_all(session_dir)?;
            } else {
                fs::remove_file(session_dir)?;
            }
        }

        // Create the symlink from session location to persistent location
        if let Err(e) = symlink(persistent_dir, session_dir) {
            warn!(
                "Failed to create symlink from {} to {}: {e}",
                session_dir.display(),
                persistent_dir.display()
            );
            // If symlink fails, create the session directory normally
            fs::create_dir_all(session_dir)?;
        } else {
            info!(
                "Created symlink: {} -> {}",
                session_dir.display(),
                persistent_dir.display()
            );
        }

        Ok(())
    }

    /// Get the URL for a specific running node (matching governance origin exactly)
    #[must_use]
    pub fn get_node_url(&self, node_id: TuiNodeId) -> Option<String> {
        let nodes = self.nodes.read();

        if let Some(NodeRecord::Running(handle)) = nodes.get(&node_id) {
            // Extract the actual port from the node configuration
            let port = handle.config.port;

            // Use the same origin format as registered in governance
            let origin = if node_id.is_first_node() {
                format!("http://localhost:{port}")
            } else {
                let pokemon_name = node_id.pokemon_name();
                format!("http://{pokemon_name}.local:{port}")
            };

            Some(origin)
        } else {
            None
        }
    }

    /// Get current node information for UI display
    ///
    /// This method reads the status from either running or stopped nodes.
    #[must_use]
    pub fn get_nodes_for_ui(&self) -> NodeUiInfo {
        // First, process any completion messages
        self.process_completion_messages();

        let nodes = self.nodes.read();
        let mut result = HashMap::new();

        for (id, record) in nodes.iter() {
            let (name, status, specializations) = match record {
                NodeRecord::Running(handle) => (
                    handle.name.clone(),
                    handle.get_status(),
                    handle.specializations.clone(),
                ),
                NodeRecord::Stopped {
                    name,
                    final_status,
                    specializations,
                    ..
                } => (name.clone(), final_status.clone(), specializations.clone()),
            };

            result.insert(*id, (name, status, specializations));
        }

        drop(nodes);

        result
    }

    /// Check if all nodes have been shut down (for shutdown coordination)
    #[allow(clippy::cognitive_complexity)]
    #[must_use]
    pub fn is_shutdown_complete(&self) -> bool {
        // First, process any completion messages
        self.process_completion_messages();

        let nodes = self.nodes.read();

        // If no nodes exist, shutdown is complete
        if nodes.is_empty() {
            info!("Shutdown complete: no nodes tracked");
            return true;
        }

        // Check if all nodes are in Stopped or Failed state
        let mut running_nodes = Vec::new();
        for (id, record) in nodes.iter() {
            match record {
                NodeRecord::Running(handle) => {
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
                NodeRecord::Stopped { final_status, .. } => {
                    info!(
                        "Node {} is shutdown: {}",
                        id.full_pokemon_name(),
                        final_status
                    );
                }
            }
        }

        let total_nodes = nodes.len();
        drop(nodes);

        if running_nodes.is_empty() {
            info!("Shutdown complete: all {total_nodes} nodes are stopped");
            true
        } else {
            info!(
                "Shutdown incomplete: {} nodes still running",
                running_nodes.len()
            );
            false
        }
    }

    /// Process completion messages and transition Running -> Stopped
    #[allow(clippy::cognitive_complexity)]
    fn process_completion_messages(&self) {
        if let Ok(receiver) = self.completion_receiver.try_lock() {
            while let Ok(message) = receiver.try_recv() {
                match message {
                    NodeManagerMessage::NodeFinished {
                        id,
                        name,
                        final_status,
                        specializations,
                    } => {
                        info!("Node {name} finished with status: {final_status}");

                        // Release persistent directories used by this node
                        {
                            let mut node_assignments = self.node_dir_assignments.write();
                            if let Some(assignments) = node_assignments.remove(&id) {
                                for (specialization_prefix, dir_num) in assignments {
                                    self.release_persistent_directory(
                                        &specialization_prefix,
                                        dir_num,
                                    );
                                    info!(
                                        "Released persistent directory {specialization_prefix}-{dir_num} for stopped node {name}"
                                    );
                                }
                            }
                        }

                        let mut nodes = self.nodes.write();
                        if let Some(record) = nodes.get_mut(&id)
                            && matches!(record, NodeRecord::Running(_))
                        {
                            *record = NodeRecord::Stopped {
                                name,
                                final_status,
                                specializations,
                            };
                            info!(
                                "Transitioned node {} to Stopped state",
                                id.full_pokemon_name()
                            );
                        }
                    }
                }
            }
        }
    }
}
