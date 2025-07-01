//! Node lifecycle management with clean interface

pub mod records;

use crate::messages::NodeOperation;
use crate::node_id::NodeId;
use records::{NodeHandle, NodeManagerMessage, NodeRecord, create_node_config};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, mpsc};

use parking_lot::RwLock;
use proven_governance_mock::MockGovernance;
use proven_local::NodeStatus;
use tracing::{error, info, warn};

/// Manages multiple node instances with a clean interface
#[derive(Clone, Debug)]
pub struct NodeManager {
    nodes: Arc<RwLock<HashMap<NodeId, NodeRecord>>>,
    governance: Arc<MockGovernance>,
    session_id: String,
    completion_sender: mpsc::Sender<NodeManagerMessage>,
    completion_receiver: Arc<std::sync::Mutex<mpsc::Receiver<NodeManagerMessage>>>,
}

impl NodeManager {
    /// Create a new node manager
    #[must_use]
    pub fn new(governance: Arc<MockGovernance>, session_id: String) -> Self {
        info!("Created new NodeManager with session_id: {}", session_id);

        let (completion_sender, completion_receiver) = mpsc::channel();

        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            governance,
            session_id,
            completion_sender,
            completion_receiver: Arc::new(std::sync::Mutex::new(completion_receiver)),
        }
    }

    /// Start a new node with optional configuration
    #[allow(clippy::cognitive_complexity)]
    /// Start a new node with specific specializations
    #[allow(clippy::cognitive_complexity)]
    pub fn start_node(
        &self,
        id: NodeId,
        name: &str,
        specializations: HashSet<proven_governance::NodeSpecialization>,
    ) {
        info!(
            "Starting node {} ({}) with specializations: {:?}",
            name,
            id.full_pokemon_name(),
            specializations
        );

        let node_config = create_node_config(id, name, &self.governance, &self.session_id);

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
                                "Failed to send start command to running node {} ({}): {}",
                                name, id, e
                            );
                        }
                        return;
                    }
                    NodeRecord::Stopped { .. } => {
                        // Node was stopped, we'll replace it with a new running instance
                        info!("Restarting previously stopped node {}", name);
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
            if let Some(NodeRecord::Running(node_handle)) = nodes_guard.get(&id) {
                if let Err(e) = node_handle.send_command(NodeOperation::Start {
                    config: Some(Box::new(node_config)),
                    specializations: Some(specializations),
                }) {
                    error!(
                        "Failed to send start command to node {} ({}): {}",
                        name, id, e
                    );
                }
            }
        }
    }

    /// Stop a node
    #[allow(clippy::cognitive_complexity)]
    pub fn stop_node(&self, id: NodeId) {
        let nodes_guard = self.nodes.read();
        if let Some(record) = nodes_guard.get(&id) {
            match record {
                NodeRecord::Running(handle) => {
                    info!("Stopping node {}", id.full_pokemon_name());
                    if let Err(e) = handle.send_command(NodeOperation::Stop) {
                        error!("Failed to send stop command to node {}: {}", id, e);
                    }
                }
                NodeRecord::Stopped { .. } => {
                    info!("Node {} is already stopped", id.full_pokemon_name());
                }
            }
        } else {
            warn!("Attempted to stop non-existent node: {}", id);
        }
    }

    /// Restart a node
    #[allow(clippy::cognitive_complexity)]
    pub fn restart_node(&self, id: NodeId) {
        let nodes_guard = self.nodes.read();
        if let Some(record) = nodes_guard.get(&id) {
            match record {
                NodeRecord::Running(handle) => {
                    info!("Restarting node {}", id.full_pokemon_name());
                    if let Err(e) = handle.send_command(NodeOperation::Restart) {
                        error!("Failed to send restart command to node {}: {}", id, e);
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
            warn!("Attempted to restart non-existent node: {}", id);
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
                            error!("Failed to send shutdown command to node {}: {}", id, e);
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

    /// Get the URL for a specific running node (matching governance origin exactly)
    pub fn get_node_url(&self, node_id: NodeId) -> Option<String> {
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
    pub fn get_nodes_for_ui(
        &self,
    ) -> HashMap<
        NodeId,
        (
            String,
            NodeStatus,
            HashSet<proven_governance::NodeSpecialization>,
        ),
    > {
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

    /// Process completion messages and transition Running -> Stopped
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
                        info!("Node {} finished with status: {}", name, final_status);

                        let mut nodes = self.nodes.write();
                        if let Some(record) = nodes.get_mut(&id) {
                            if matches!(record, NodeRecord::Running(_)) {
                                *record = NodeRecord::Stopped {
                                    _id: id,
                                    name,
                                    final_status,
                                    specializations,
                                    _stopped_at: std::time::Instant::now(),
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
}
