//! Adapter layer between TUI and LocalCluster
//!
//! This module bridges the gap between the TUI's Pokemon-based node identification
//! and interactive features with LocalCluster's programmatic API.

use crate::node_id::TuiNodeId;
use anyhow::{Result, anyhow};
use parking_lot::RwLock;
use proven_local::NodeStatus;
use proven_local_cluster::{ClusterBuilder, LocalCluster, LogFilter, RpcClient, UserIdentity};
use proven_topology::{NodeId, NodeSpecialization};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing::info;

/// Maps between TUI node IDs and cluster node IDs
#[derive(Debug, Clone)]
struct NodeMapping {
    cluster_id: NodeId,
    display_name: String,
}

/// Adapter between TUI and LocalCluster
pub struct ClusterAdapter {
    /// The underlying cluster
    cluster: LocalCluster,

    /// Tokio runtime for async operations
    runtime: Arc<Runtime>,

    /// Mapping between TUI and cluster node IDs
    node_mappings: Arc<RwLock<HashMap<TuiNodeId, NodeMapping>>>,

    /// Reverse mapping for quick lookups
    cluster_to_tui: Arc<RwLock<HashMap<NodeId, TuiNodeId>>>,

    /// Session ID for this TUI instance
    session_id: String,
}

impl ClusterAdapter {
    /// Create a new cluster adapter
    pub fn new(session_id: String, log_dir: &Path) -> Result<Self> {
        info!("Creating ClusterAdapter with session_id: {}", session_id);

        // Create tokio runtime for async operations
        let runtime = Arc::new(
            tokio::runtime::Runtime::new()
                .map_err(|e| anyhow!("Failed to create tokio runtime: {}", e))?,
        );

        // Build the cluster with TUI-specific configuration
        let cluster = runtime.block_on(async {
            ClusterBuilder::default()
                .with_session_id(session_id.clone())
                .with_log_dir(log_dir.to_path_buf())
                // Stdout logging disabled for TUI - it interferes with the interface
                .build()
                .await
        })?;

        // Setup the cluster's tracing subscriber for global logging
        if let Err(e) = cluster.get_log_system().setup_global_subscriber() {
            // If setting up the global subscriber fails, it might already be set
            // This is not a fatal error for TUI functionality
            eprintln!("Warning: Failed to setup global tracing subscriber: {e}");
        }

        Ok(Self {
            cluster,
            runtime,
            node_mappings: Arc::new(RwLock::new(HashMap::new())),
            cluster_to_tui: Arc::new(RwLock::new(HashMap::new())),
            session_id,
        })
    }

    /// Start a new node with Pokemon naming
    pub fn start_node(
        &mut self,
        tui_id: TuiNodeId,
        name: &str,
        specializations: HashSet<NodeSpecialization>,
    ) -> Result<()> {
        info!(
            "Starting node {} ({}) with specializations: {:?}",
            name,
            tui_id.full_pokemon_name(),
            specializations
        );

        // Create node in cluster with specializations
        let node_info = self
            .cluster
            .create_node_with_specializations(name, specializations)?;

        // Store mapping
        let cluster_id = node_info.id;
        let mapping = NodeMapping {
            cluster_id,
            display_name: name.to_string(),
        };

        self.node_mappings.write().insert(tui_id, mapping.clone());
        self.cluster_to_tui.write().insert(cluster_id, tui_id);

        // ALWAYS add to topology BEFORE starting (required by new system)
        // The node needs to be in topology to find itself during startup
        self.runtime
            .block_on(async { self.cluster.add_to_topology(&cluster_id).await })?;

        // Start the node after adding to topology
        self.runtime
            .block_on(async { self.cluster.start_node(&cluster_id).await })?;

        Ok(())
    }

    /// Stop a node
    pub fn stop_node(&mut self, tui_id: TuiNodeId) -> Result<()> {
        let mappings = self.node_mappings.read();
        let mapping = mappings
            .get(&tui_id)
            .ok_or_else(|| anyhow!("Node {} not found", tui_id))?;

        let cluster_id = mapping.cluster_id;
        drop(mappings);

        info!("Stopping node {}", tui_id.full_pokemon_name());

        self.runtime
            .block_on(async { self.cluster.stop_node(&cluster_id).await })?;

        Ok(())
    }

    /// Restart a node
    pub fn restart_node(&mut self, tui_id: TuiNodeId) -> Result<()> {
        let mappings = self.node_mappings.read();
        let mapping = mappings
            .get(&tui_id)
            .ok_or_else(|| anyhow!("Node {} not found", tui_id))?;

        let cluster_id = mapping.cluster_id;
        drop(mappings);

        info!("Restarting node {}", tui_id.full_pokemon_name());

        self.runtime
            .block_on(async { self.cluster.restart_node(&cluster_id).await })?;

        Ok(())
    }

    /// Shutdown all nodes
    pub fn shutdown_all(&mut self) -> Result<()> {
        info!("Shutting down all nodes...");

        self.runtime
            .block_on(async { self.cluster.shutdown_all().await })?;

        Ok(())
    }

    /// Get node information for UI display
    pub fn get_nodes_for_ui(
        &self,
    ) -> HashMap<TuiNodeId, (String, NodeStatus, HashSet<NodeSpecialization>)> {
        let mut result = HashMap::new();
        let mappings = self.node_mappings.read();

        for (tui_id, mapping) in mappings.iter() {
            // Get node status from cluster
            if let Some(status) = self.cluster.get_node_status(&mapping.cluster_id) {
                // Get specializations from topology
                let specializations = self.runtime.block_on(async {
                    // Get topology and find this node's specializations
                    if let Ok(topology) = self.cluster.get_topology().await {
                        topology
                            .iter()
                            .find(|(id, _)| id == &mapping.cluster_id)
                            .map(|(_, node)| node.specializations.clone())
                            .unwrap_or_default()
                    } else {
                        HashSet::new()
                    }
                });

                result.insert(
                    *tui_id,
                    (mapping.display_name.clone(), status, specializations),
                );
            }
        }

        result
    }

    /// Get the URL for a specific node
    pub fn get_node_url(&self, tui_id: TuiNodeId) -> Option<String> {
        let mappings = self.node_mappings.read();
        let mapping = mappings.get(&tui_id)?;

        // Get all nodes and find the port for this one
        let all_nodes = self.cluster.get_all_nodes();
        all_nodes
            .get(&mapping.cluster_id)
            .map(|info| format!("http://localhost:{}", info.port))
    }

    /// Check if all nodes have shut down
    pub fn is_shutdown_complete(&self) -> bool {
        // Check if all nodes are stopped or failed
        let all_nodes = self.cluster.get_all_nodes();
        for node_id in all_nodes.keys() {
            if let Some(status) = self.cluster.get_node_status(node_id) {
                match status {
                    NodeStatus::Stopped | NodeStatus::Failed(_) => continue,
                    _ => return false,
                }
            }
        }
        true
    }

    /// Create an RPC client for a specific user
    pub fn create_rpc_client(&self, user_name: &str) -> Result<RpcClient> {
        let identity = UserIdentity::new(user_name);
        let mut client = RpcClient::new();
        client.set_identity(identity);
        Ok(client)
    }

    /// Get logs from the cluster
    pub fn get_logs(&self, limit: Option<usize>) -> Result<Vec<proven_local_cluster::LogEntry>> {
        let filter = LogFilter {
            node_id: None,
            level: None,
            since: None,
            limit,
        };
        self.cluster.get_logs(&filter)
    }

    /// Get the log database path for direct SQLite access
    pub fn get_log_db_path(&self) -> Option<PathBuf> {
        // LocalCluster stores logs in session_dir/logs.db
        let session_dir = Path::new("/tmp/proven").join(&self.session_id);
        let log_db = session_dir.join("logs.db");
        if log_db.exists() {
            Some(log_db)
        } else {
            // Try to create an empty database if it doesn't exist yet
            if let Ok(()) = std::fs::create_dir_all(&session_dir) {
                // Just return the path, the log reader will create the DB if needed
                Some(log_db)
            } else {
                None
            }
        }
    }

    /// Clear all logs from the database
    pub fn clear_logs(&mut self) -> Result<()> {
        self.cluster.clear_logs()
    }
}
