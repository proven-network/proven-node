//! Main cluster management implementation

use crate::{
    config::create_node_config,
    logging::{ClusterLogSystem, LogEntry, LogFilter},
    node::{ManagedNode, NodeInfo, NodeMessage, NodeOperation, generate_deterministic_signing_key},
    persistence::PersistentDirManager,
    rpc::{RpcClient, UserIdentity},
};

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use parking_lot::RwLock;
use proven_attestation::Attestor;
use proven_attestation_mock::MockAttestor;
use proven_local::NodeStatus;
use proven_topology::{Node, NodeId, NodeSpecialization, TopologyAdaptor, Version};
use proven_topology_mock::MockTopologyAdaptor;
use proven_util::port_allocator::allocate_port;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Main cluster manager
pub struct LocalCluster {
    /// Topology managed independently
    governance: Arc<MockTopologyAdaptor>,

    /// Nodes tracked separately from topology
    nodes: Arc<DashMap<NodeId, ManagedNode>>,

    /// Session ID for this cluster
    session_id: String,

    /// Logging system
    log_system: Arc<ClusterLogSystem>,

    /// Persistent directory manager
    #[allow(dead_code)]
    persistent_dirs: Arc<RwLock<PersistentDirManager>>,

    /// Message receiver for node completion
    #[allow(dead_code)]
    completion_receiver: mpsc::Receiver<NodeMessage>,

    /// Message sender for node completion
    completion_sender: mpsc::Sender<NodeMessage>,

    /// Counter for execution order
    next_execution_order: Arc<RwLock<u32>>,
}

impl LocalCluster {
    /// Create a new cluster builder
    #[must_use]
    pub fn builder() -> ClusterBuilder {
        ClusterBuilder::default()
    }

    /// Create a new cluster (internal - use builder)
    fn new(
        governance: MockTopologyAdaptor,
        session_id: String,
        log_dir: &Path,
        enable_stdout_logging: bool,
    ) -> Result<Self> {
        let log_system = if enable_stdout_logging {
            ClusterLogSystem::with_stdout(log_dir)?
        } else {
            ClusterLogSystem::new(log_dir)?
        };
        let (completion_sender, completion_receiver) = mpsc::channel(32);

        Ok(Self {
            governance: Arc::new(governance),
            nodes: Arc::new(DashMap::new()),
            session_id,
            log_system: Arc::new(log_system),
            persistent_dirs: Arc::new(RwLock::new(PersistentDirManager::new())),
            completion_receiver,
            completion_sender,
            next_execution_order: Arc::new(RwLock::new(1)),
        })
    }

    // --- Node Management ---

    /// Create a node configuration without starting it
    ///
    /// # Errors
    ///
    /// Returns an error if port allocation fails or node creation fails
    pub fn create_node(&mut self, name: &str) -> Result<NodeInfo> {
        self.create_node_with_specializations(name, HashSet::default())
    }

    /// Create a node configuration with specializations without starting it
    ///
    /// # Errors
    ///
    /// Returns an error if port allocation fails or node creation fails
    pub fn create_node_with_specializations(
        &mut self,
        name: &str,
        specializations: HashSet<NodeSpecialization>,
    ) -> Result<NodeInfo> {
        let execution_order = {
            let mut order = self.next_execution_order.write();
            let current = *order;
            *order += 1;
            current
        };

        let signing_key = generate_deterministic_signing_key(execution_order);
        let node_id = NodeId::from(signing_key.verifying_key());
        let port = allocate_port();

        let info = NodeInfo {
            id: node_id.clone(),
            name: name.to_string(),
            port,
            signing_key: signing_key.clone(),
            execution_order,
        };

        let config = create_node_config(
            execution_order,
            name,
            port,
            &self.governance,
            &self.session_id,
            signing_key,
        );

        let node = ManagedNode::new(
            info.clone(),
            config,
            specializations,
            self.governance.clone(),
            self.completion_sender.clone(),
        )?;

        self.nodes.insert(node_id, node);

        Ok(info)
    }

    /// Start a previously created node
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or the command fails
    pub async fn start_node(&mut self, id: &NodeId) -> Result<()> {
        self.nodes
            .get(id)
            .ok_or_else(|| anyhow!("Node {} not found", id))?
            .send_command(NodeOperation::Start {
                specializations: None,
            })
            .await?;

        Ok(())
    }

    /// Stop a running node
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or the command fails
    pub async fn stop_node(&mut self, id: &NodeId) -> Result<()> {
        self.nodes
            .get(id)
            .ok_or_else(|| anyhow!("Node {} not found", id))?
            .send_command(NodeOperation::Stop)
            .await?;

        Ok(())
    }

    /// Restart a node
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or the command fails
    pub async fn restart_node(&mut self, id: &NodeId) -> Result<()> {
        self.nodes
            .get(id)
            .ok_or_else(|| anyhow!("Node {} not found", id))?
            .send_command(NodeOperation::Restart)
            .await?;

        Ok(())
    }

    /// Remove a node completely
    ///
    /// # Errors
    ///
    /// Returns an error if sending the shutdown command fails
    pub async fn remove_node(&mut self, id: &NodeId) -> Result<()> {
        // Send shutdown command first
        if let Some(node) = self.nodes.get(id) {
            node.send_command(NodeOperation::Shutdown).await?;
        }

        // Wait a bit for shutdown
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Remove from nodes
        self.nodes.remove(id);

        // Remove from topology if present
        let _ = self.governance.remove_node(*id.verifying_key());

        Ok(())
    }

    /// Get node status
    #[must_use]
    pub fn get_node_status(&self, id: &NodeId) -> Option<NodeStatus> {
        self.nodes.get(id).map(|entry| entry.get_status())
    }

    /// Get all node information
    #[must_use]
    pub fn get_all_nodes(&self) -> HashMap<NodeId, NodeInfo> {
        self.nodes
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().info.clone()))
            .collect()
    }

    // --- Topology Management ---

    /// Add a node to topology (node can be running or not)
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or adding to topology fails
    pub async fn add_to_topology(&self, node_id: &NodeId) -> Result<()> {
        let (port, specializations, needs_start) = {
            let node = self
                .nodes
                .get(node_id)
                .ok_or_else(|| anyhow!("Node {} not found", node_id))?;

            let port = node.info.port;
            let specializations = node.specializations.clone();
            let needs_start = matches!(node.get_status(), NodeStatus::Running);
            drop(node);
            (port, specializations, needs_start)
        };

        let topology_node = Node::new(
            "local".to_string(),
            format!("http://localhost:{port}"),
            node_id.clone(),
            "local".to_string(),
            specializations.clone(),
        );

        self.governance.add_node(topology_node)?;

        // If the node is running, send it a start command with specializations
        // so it knows it's in the topology
        if needs_start && let Some(node) = self.nodes.get(node_id) {
            node.send_command(NodeOperation::Start {
                specializations: Some(specializations),
            })
            .await?;
        }

        Ok(())
    }

    /// Remove a node from topology (node keeps running if it was)
    ///
    /// # Errors
    ///
    /// Returns an error if removing from topology fails
    pub fn remove_from_topology(&self, node_id: &NodeId) -> Result<()> {
        self.governance.remove_node(*node_id.verifying_key())?;
        Ok(())
    }

    /// Get current topology
    ///
    /// # Errors
    ///
    /// Returns an error if fetching topology fails
    pub async fn get_topology(&self) -> Result<Vec<(NodeId, Node)>> {
        let nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| anyhow!("Failed to get topology: {:?}", e))?;
        Ok(nodes
            .into_iter()
            .map(|node| {
                let node_id = node.node_id.clone();
                (node_id, node)
            })
            .collect())
    }

    /// Clear all nodes from topology
    ///
    /// # Errors
    ///
    /// Returns an error if getting topology or removing nodes fails
    pub async fn clear_topology(&self) -> Result<()> {
        let nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| anyhow!("Failed to get topology: {:?}", e))?;
        for node in nodes {
            // Find node in our tracking by ID
            let node_id = node.node_id.clone();
            let info = self
                .nodes
                .iter()
                .find(|entry| entry.value().info.id == node_id)
                .map(|entry| entry.value().info.clone());

            if let Some(info) = info {
                self.governance
                    .remove_node(*info.id.verifying_key())
                    .map_err(|e| anyhow!("Failed to remove node: {:?}", e))?;
            }
        }
        Ok(())
    }

    // --- RPC Support ---

    /// Create an RPC client for a specific user and node
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or client initialization fails
    pub async fn create_rpc_client(
        &self,
        node_id: &NodeId,
        user: Option<UserIdentity>,
    ) -> Result<RpcClient> {
        let node_url = format!(
            "http://localhost:{}",
            self.nodes
                .get(node_id)
                .ok_or_else(|| anyhow!("Node {} not found", node_id))?
                .info
                .port
        );

        let mut client = RpcClient::new();

        if let Some(user_identity) = user {
            client.set_identity(user_identity);
        }

        client.initialize_session(&node_url).await?;

        Ok(client)
    }

    /// Create an anonymous RPC client
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or client initialization fails
    pub async fn create_anonymous_client(&self, node_id: &NodeId) -> Result<RpcClient> {
        self.create_rpc_client(node_id, None).await
    }

    /// Create multiple test users
    #[must_use]
    pub fn create_test_users(names: &[&str]) -> Vec<UserIdentity> {
        names.iter().map(|name| UserIdentity::new(name)).collect()
    }

    // --- Logging ---

    /// Get logs with filter
    ///
    /// # Errors
    ///
    /// Returns an error if querying logs fails
    pub fn get_logs(&self, filter: &LogFilter) -> Result<Vec<LogEntry>> {
        let conn = self.log_system.get_connection();
        let conn = conn.lock();
        filter.query_logs(&conn)
    }

    /// Get logs for specific node
    ///
    /// # Errors
    ///
    /// Returns an error if querying logs fails
    pub fn get_node_logs(&self, node_id: &NodeId) -> Result<Vec<LogEntry>> {
        // Find the node's execution order to build the proper filter
        let execution_order = self
            .nodes
            .get(node_id)
            .ok_or_else(|| anyhow!("Node {} not found", node_id))?
            .info
            .execution_order;

        // Use a prefix of the node ID for matching
        let node_id_str = node_id.to_string();
        let short_id = &node_id_str[..8.min(node_id_str.len())];
        let thread_pattern = format!("node-{}-{}-{}", self.session_id, execution_order, short_id);

        let filter = LogFilter {
            node_id: Some(thread_pattern),
            ..Default::default()
        };
        self.get_logs(&filter)
    }

    /// Export logs to file
    ///
    /// # Errors
    ///
    /// Returns an error if getting logs or writing to file fails
    pub fn export_logs(&self, path: &Path) -> Result<()> {
        let logs = self.get_logs(&LogFilter::default())?;
        let json = serde_json::to_string_pretty(&logs)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    // --- Utility Methods ---

    /// Wait for all nodes to shutdown
    ///
    /// # Errors
    ///
    /// Returns an error if timeout is exceeded
    pub async fn wait_for_shutdown(&self, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow!("Timeout waiting for nodes to shutdown"));
            }

            let all_stopped = self.nodes.iter().all(|entry| {
                matches!(
                    entry.value().get_status(),
                    NodeStatus::Stopped | NodeStatus::NotStarted | NodeStatus::Failed(_)
                )
            });

            if all_stopped {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Shutdown all nodes
    ///
    /// # Errors
    ///
    /// Returns an error if waiting for shutdown times out
    pub async fn shutdown_all(&mut self) -> Result<()> {
        // Send shutdown to all nodes
        for node in self.nodes.iter() {
            let _ = node.send_command(NodeOperation::Shutdown).await;
        }

        // Wait for shutdown
        self.wait_for_shutdown(Duration::from_secs(30)).await?;

        // Clear nodes
        self.nodes.clear();

        Ok(())
    }

    /// Get the node data path
    #[must_use]
    pub fn get_node_data_path(&self, node_id: &NodeId) -> Option<PathBuf> {
        self.nodes.get(node_id).map(|entry| {
            PathBuf::from(format!(
                "/tmp/proven/{}/data/{}",
                self.session_id, entry.info.name
            ))
        })
    }

    /// Get the session ID for this cluster
    #[must_use]
    pub fn get_session_id(&self) -> &str {
        &self.session_id
    }

    /// Get the log system for setting up tracing
    #[must_use]
    pub fn get_log_system(&self) -> &ClusterLogSystem {
        &self.log_system
    }
}

/// Builder for creating a `LocalCluster`
#[derive(Default)]
pub struct ClusterBuilder {
    session_id: Option<String>,
    base_log_dir: Option<PathBuf>,
    enable_persistence: bool,
    enable_stdout_logging: bool,
}

impl ClusterBuilder {
    /// Set the session ID
    #[must_use]
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Set the base log directory
    #[must_use]
    pub fn with_log_dir(mut self, dir: PathBuf) -> Self {
        self.base_log_dir = Some(dir);
        self
    }

    /// Enable persistence
    #[must_use]
    pub const fn enable_persistence(mut self) -> Self {
        self.enable_persistence = true;
        self
    }

    /// Enable stdout logging (useful for debugging)
    #[must_use]
    pub const fn with_stdout_logging(mut self) -> Self {
        self.enable_stdout_logging = true;
        self
    }

    /// Build the cluster
    ///
    /// # Errors
    ///
    /// Returns an error if creating the cluster fails
    pub async fn build(self) -> Result<LocalCluster> {
        let session_id = self
            .session_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let log_dir = self
            .base_log_dir
            .unwrap_or_else(|| PathBuf::from(format!("/tmp/proven/{session_id}/logs")));

        // Create mock attestor and governance
        let attestor = MockAttestor::new();
        let pcrs = attestor.pcrs().await?;
        let version = Version::from_pcrs(pcrs);

        let governance = MockTopologyAdaptor::new(
            vec![],
            vec![version],
            "http://localhost".to_string(),
            vec![],
        );

        LocalCluster::new(governance, session_id, &log_dir, self.enable_stdout_logging)
    }
}
