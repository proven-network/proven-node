//! Node management for cluster operations

use anyhow::Result;
use ed25519_dalek::{SECRET_KEY_LENGTH, SigningKey};
use parking_lot::RwLock;
use proven_local::{LocalNode, NodeStatus};
use proven_topology::{Node, NodeId, NodeSpecialization};
use proven_topology_mock::MockTopologyAdaptor;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

/// Per-node operations
#[derive(Debug, Clone)]
pub enum NodeOperation {
    /// Start the node
    Start {
        /// Specializations for this node (used for governance registration)
        specializations: Option<HashSet<NodeSpecialization>>,
    },
    /// Stop the node
    Stop,
    /// Restart the node (stop then start)
    Restart,
    /// Shutdown the node permanently
    Shutdown,
}

/// Node information
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Node ID
    pub id: NodeId,
    /// Display name
    pub name: String,
    /// Port number
    pub port: u16,
    /// Signing key
    pub signing_key: SigningKey,
    /// Execution order (for thread naming)
    pub execution_order: u32,
}

/// Message sent from node thread back to cluster manager
#[derive(Debug)]
pub enum NodeMessage {
    NodeFinished {
        id: NodeId,
        name: String,
        final_status: NodeStatus,
        specializations: HashSet<NodeSpecialization>,
    },
}

/// Managed node with its metadata and command processing
#[derive(Debug)]
pub struct ManagedNode {
    /// Node information
    pub info: NodeInfo,

    /// Node configuration
    pub config: proven_local::NodeConfig<MockTopologyAdaptor>,

    /// Specializations for this node
    pub specializations: HashSet<NodeSpecialization>,

    /// Command sender for this specific node
    pub command_sender: mpsc::Sender<NodeOperation>,

    /// Status of the node
    pub status: Arc<RwLock<NodeStatus>>,

    /// Thread handle for the node manager (contains its own runtime)
    thread_handle: Option<thread::JoinHandle<()>>,

    /// Shutdown signal sender
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl ManagedNode {
    /// Create a new managed node
    ///
    /// # Errors
    ///
    /// Returns an error if creating the runtime fails
    ///
    /// # Panics
    ///
    /// Panics if the tokio runtime cannot be built in the spawned thread
    pub fn new(
        info: NodeInfo,
        config: proven_local::NodeConfig<MockTopologyAdaptor>,
        specializations: HashSet<NodeSpecialization>,
        governance: Arc<MockTopologyAdaptor>,
        completion_sender: mpsc::Sender<NodeMessage>,
    ) -> Result<Self> {
        let (command_sender, command_receiver) = mpsc::channel(32);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();

        // Extract session ID from rocksdb_store_dir path: /tmp/proven/{session_id}/...
        let session_prefix = config
            .rocksdb_store_dir
            .to_str()
            .and_then(|p| p.split('/').nth(3))
            .unwrap_or("unknown");
        let node_id_str = info.id.to_string();
        let short_id = &node_id_str[..8.min(node_id_str.len())];

        // Shared status
        let status = Arc::new(RwLock::new(NodeStatus::NotStarted));

        // Spawn command processor in a dedicated thread with its own runtime
        let info_clone = info.clone();
        let config_clone = config.clone();
        let specializations_clone = specializations.clone();
        let status_clone = status.clone();
        let thread_name = format!(
            "node-{}-{}-{}",
            session_prefix, info.execution_order, short_id
        );

        let thread_handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                // Create dedicated runtime for this node inside the thread
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(8)
                    .thread_name(thread_name)
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime");

                // Run the command processor on this runtime
                runtime.block_on(async move {
                    Self::command_processor(
                        info_clone,
                        config_clone,
                        command_receiver,
                        governance,
                        status_clone,
                        completion_sender,
                        specializations_clone,
                        shutdown_receiver,
                    )
                    .await;
                });
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn thread: {}", e))?;

        Ok(Self {
            info,
            config,
            specializations,
            command_sender,
            status,
            thread_handle: Some(thread_handle),
            shutdown_sender: Some(shutdown_sender),
        })
    }

    /// Send a command to this node
    ///
    /// # Errors
    ///
    /// Returns an error if the command channel is disconnected
    pub async fn send_command(&self, operation: NodeOperation) -> Result<()> {
        self.command_sender
            .send(operation)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send command: {}", e))
    }

    /// Get the current status
    #[must_use]
    pub fn get_status(&self) -> NodeStatus {
        self.status.read().clone()
    }

    /// Command processor task
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_arguments)]
    async fn command_processor(
        info: NodeInfo,
        config: proven_local::NodeConfig<MockTopologyAdaptor>,
        mut command_receiver: mpsc::Receiver<NodeOperation>,
        governance: Arc<MockTopologyAdaptor>,
        status: Arc<RwLock<NodeStatus>>,
        completion_sender: mpsc::Sender<NodeMessage>,
        specializations: HashSet<NodeSpecialization>,
        mut shutdown_receiver: oneshot::Receiver<()>,
    ) {
        // Create node instance
        let mut node = LocalNode::new(config);

        info!("Started command processor for node {}", info.name);

        // Process commands
        loop {
            tokio::select! {
                Some(operation) = command_receiver.recv() => {
                    match operation {
                        NodeOperation::Start { specializations: specs } => {
                            Self::handle_start(
                                &mut node,
                                &info,
                                &governance,
                                &status,
                                specs.as_ref(),
                            ).await;
                        }
                        NodeOperation::Stop => {
                            Self::handle_stop(&mut node, &info, &governance, &status).await;
                        }
                        NodeOperation::Restart => {
                            Self::handle_stop(&mut node, &info, &governance, &status).await;
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            Self::handle_start(&mut node, &info, &governance, &status, None).await;
                        }
                        NodeOperation::Shutdown => {
                            Self::handle_stop(&mut node, &info, &governance, &status).await;
                            break;
                        }
                    }
                }
                _ = &mut shutdown_receiver => {
                    info!("Node {} received shutdown signal", info.name);
                    Self::handle_stop(&mut node, &info, &governance, &status).await;
                    break;
                }
                () = tokio::time::sleep(Duration::from_millis(250)) => {
                    // Sync status periodically
                    Self::sync_status(&node, &status).await;

                    // Check for terminal states
                    let current_status = status.read().clone();
                    if matches!(current_status, NodeStatus::Failed(_) | NodeStatus::Stopped) {
                        info!("Node {} in terminal state, shutting down", info.name);
                        break;
                    }
                }
            }
        }

        info!("Command processor for node {} shutting down", info.name);

        // Send completion message
        let final_status = status.read().clone();
        let _ = completion_sender
            .send(NodeMessage::NodeFinished {
                id: info.id,
                name: info.name.clone(),
                final_status,
                specializations,
            })
            .await;
    }

    /// Sync status with node
    async fn sync_status(node: &LocalNode<MockTopologyAdaptor>, status: &Arc<RwLock<NodeStatus>>) {
        let current_status = node.status().await;
        let mut status_guard = status.write();
        if *status_guard != current_status {
            *status_guard = current_status;
        }
    }

    /// Handle start operation
    #[allow(clippy::cognitive_complexity)]
    async fn handle_start(
        node: &mut LocalNode<MockTopologyAdaptor>,
        info: &NodeInfo,
        governance: &Arc<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
        specializations: Option<&HashSet<NodeSpecialization>>,
    ) {
        {
            let mut status_guard = status.write();
            *status_guard = NodeStatus::Starting;
        }

        // Check if already running
        let current_status = node.status().await;
        if matches!(current_status, NodeStatus::Starting | NodeStatus::Running) {
            info!("Node {} already starting/running", info.name);
            status.write().clone_from(&current_status);
            return;
        }

        // Only add to governance if we're asked to (independent topology control)
        if let Some(specs) = specializations {
            let origin = format!("http://localhost:{}", info.port);
            let topology_node = Node::new(
                "local".to_string(),
                origin,
                info.id.clone(),
                "local".to_string(),
                specs.clone(),
            );

            if let Err(e) = governance.add_node(topology_node) {
                error!("Failed to add node to governance: {}", e);
                status.write().clone_from(&NodeStatus::Failed(format!(
                    "Failed to add to governance: {e}"
                )));
                return;
            }
        }

        // Start the node
        let start_result = node.start().await;

        if let Err(e) = start_result {
            error!("Failed to start node {}: {}", info.name, e);
            status
                .write()
                .clone_from(&NodeStatus::Failed(format!("Start failed: {e}")));
            return;
        }

        info!("Node {} started successfully", info.name);
        Self::sync_status(node, status).await;
    }

    /// Handle stop operation
    #[allow(clippy::cognitive_complexity)]
    async fn handle_stop(
        node: &mut LocalNode<MockTopologyAdaptor>,
        info: &NodeInfo,
        governance: &Arc<MockTopologyAdaptor>,
        status: &Arc<RwLock<NodeStatus>>,
    ) {
        {
            let mut status_guard = status.write();
            *status_guard = NodeStatus::Stopping;
        }

        let current_status = node.status().await;
        if matches!(
            current_status,
            NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Stopping
        ) {
            info!("Node {} not running", info.name);
            status.write().clone_from(&current_status);
            return;
        }

        // Remove from governance
        if let Err(e) = governance.remove_node(*info.id.verifying_key()) {
            warn!("Failed to remove node from governance: {}", e);
        }

        // Stop the node
        let stop_result = node.stop().await;

        let result = match stop_result {
            Ok(()) => {
                let final_status = node.status().await;
                Ok(final_status)
            }
            Err(e) => Err(e),
        };

        match result {
            Ok(final_status) => {
                info!("Node {} stopped successfully", info.name);
                status.write().clone_from(&final_status);
            }
            Err(e) => {
                error!("Failed to stop node {}: {}", info.name, e);
                status
                    .write()
                    .clone_from(&NodeStatus::Failed(format!("Stop failed: {e}")));
            }
        }
    }
}

impl Clone for ManagedNode {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            config: self.config.clone(),
            specializations: self.specializations.clone(),
            command_sender: self.command_sender.clone(),
            status: self.status.clone(),
            thread_handle: None,   // Don't clone the thread handle
            shutdown_sender: None, // Don't clone the shutdown sender
        }
    }
}

impl Drop for ManagedNode {
    fn drop(&mut self) {
        // Send shutdown signal if available
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }

        // Wait for the thread to finish
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Generate a deterministic signing key based on execution order
pub fn generate_deterministic_signing_key(execution_order: u32) -> SigningKey {
    let mut hasher = Sha256::new();
    hasher.update(b"proven-node-deterministic-key");
    hasher.update(execution_order.to_le_bytes());

    let hash = hasher.finalize();
    let mut seed = [0u8; SECRET_KEY_LENGTH];
    seed.copy_from_slice(&hash[..SECRET_KEY_LENGTH]);

    SigningKey::from_bytes(&seed)
}

/// Load a persisted signing key
#[allow(dead_code)]
pub fn load_persisted_signing_key(path: &PathBuf) -> Option<SigningKey> {
    match std::fs::read(path) {
        Ok(key_bytes) if key_bytes.len() == SECRET_KEY_LENGTH => {
            let mut key_array = [0u8; SECRET_KEY_LENGTH];
            key_array.copy_from_slice(&key_bytes);
            Some(SigningKey::from_bytes(&key_array))
        }
        Ok(key_bytes) => {
            error!(
                "Invalid key length: expected {}, got {}",
                SECRET_KEY_LENGTH,
                key_bytes.len()
            );
            None
        }
        Err(e) => {
            error!("Failed to read signing key: {}", e);
            None
        }
    }
}

/// Save a signing key
#[allow(dead_code)]
pub fn save_signing_key(path: &PathBuf, key: &SigningKey) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, key.to_bytes())?;
    Ok(())
}
