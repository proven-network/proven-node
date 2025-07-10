//! Read-only view of node state and capacity
//!
//! The NodeView provides read-only access to node state for the orchestrator.
//! It tracks:
//! - Node status and health
//! - Node capacity metrics
//! - Node-to-group assignments
//!
//! All actual modifications must go through consensus operations.

use crate::{
    Node, NodeId,
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error, NodeError},
    global::global_state::GlobalState,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Node status
#[derive(Debug, Clone, PartialEq)]
pub enum NodeStatus {
    /// Node is online and healthy
    Online,
    /// Node is suspected to be offline (no recent heartbeat)
    Suspected {
        /// The time the node was last seen
        last_seen: SystemTime,
    },
    /// Node is confirmed offline
    Offline {
        /// The time the node was confirmed offline
        since: SystemTime,
    },
    /// Node is in maintenance mode
    Maintenance,
    /// Node is being decommissioned
    Decommissioning,
}

/// Node capacity metrics
#[derive(Debug, Clone, Default)]
pub struct NodeCapacity {
    /// CPU usage percentage (0-100)
    pub cpu_usage: f32,
    /// Memory usage percentage (0-100)
    pub memory_usage: f32,
    /// Disk usage percentage (0-100)
    pub disk_usage: f32,
    /// Network bandwidth usage percentage (0-100)
    pub network_usage: f32,
    /// Number of active connections
    pub connection_count: usize,
    /// Number of groups this node participates in
    pub group_count: usize,
    /// Overall capacity score (0.0 = full capacity, 1.0 = no capacity)
    pub capacity_score: f64,
}

/// Node registration info
#[derive(Debug, Clone)]
pub struct NodeRegistration {
    /// Node identifier
    pub node_id: NodeId,
    /// Node metadata
    pub node: Node,
    /// Registration time
    pub registered_at: SystemTime,
    /// Last heartbeat time
    pub last_heartbeat: SystemTime,
    /// Node status
    pub status: NodeStatus,
    /// Groups this node belongs to
    pub groups: HashSet<ConsensusGroupId>,
}

/// Node view configuration
#[derive(Debug, Clone)]
pub struct NodeViewConfig {
    /// Heartbeat timeout before marking node as suspected
    pub heartbeat_timeout: Duration,
    /// Timeout before marking suspected node as offline
    pub offline_timeout: Duration,
    /// Maximum groups per node
    pub max_groups_per_node: usize,
}

impl Default for NodeViewConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            offline_timeout: Duration::from_secs(120),
            max_groups_per_node: 10,
        }
    }
}

/// Read-only view of node state for orchestrator decision-making
pub struct NodeView {
    /// Reference to global state
    _global_state: Arc<GlobalState>,
    /// Registered nodes
    nodes: Arc<RwLock<HashMap<NodeId, NodeRegistration>>>,
    /// Node capacity tracking
    node_capacity: Arc<RwLock<HashMap<NodeId, NodeCapacity>>>,
    /// Configuration
    config: NodeViewConfig,
}

impl NodeView {
    /// Create a new NodeView
    pub fn new(global_state: Arc<GlobalState>, config: NodeViewConfig) -> Self {
        Self {
            _global_state: global_state,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            node_capacity: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a new node
    pub async fn register_node(&self, node_id: NodeId, node: Node) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        if nodes.contains_key(&node_id) {
            return Err(Error::InvalidOperation(format!(
                "Node {} is already registered",
                node_id
            )));
        }

        let registration = NodeRegistration {
            node_id: node_id.clone(),
            node,
            registered_at: SystemTime::now(),
            last_heartbeat: SystemTime::now(),
            status: NodeStatus::Online,
            groups: HashSet::new(),
        };

        nodes.insert(node_id.clone(), registration);

        // Initialize capacity
        self.node_capacity
            .write()
            .await
            .insert(node_id.clone(), NodeCapacity::default());

        info!("Registered node {}", node_id);
        Ok(())
    }

    /// Deregister a node
    pub async fn deregister_node(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        let registration = nodes.get(node_id).ok_or_else(|| {
            Error::Node(NodeError::NotFound {
                id: node_id.clone(),
            })
        })?;

        // Check if node is in any groups
        if !registration.groups.is_empty() {
            return Err(Error::InvalidOperation(format!(
                "Node {} is still in {} groups",
                node_id,
                registration.groups.len()
            )));
        }

        nodes.remove(node_id);
        self.node_capacity.write().await.remove(node_id);

        info!("Deregistered node {}", node_id);
        Ok(())
    }

    /// Update node heartbeat
    pub async fn update_heartbeat(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        let registration = nodes.get_mut(node_id).ok_or_else(|| {
            Error::Node(NodeError::NotFound {
                id: node_id.clone(),
            })
        })?;

        registration.last_heartbeat = SystemTime::now();

        // If node was suspected or offline, mark as online
        match registration.status {
            NodeStatus::Suspected { .. } | NodeStatus::Offline { .. } => {
                info!("Node {} is back online", node_id);
                registration.status = NodeStatus::Online;
            }
            _ => {}
        }

        Ok(())
    }

    /// Update node capacity
    pub async fn update_capacity(&self, node_id: &NodeId, capacity: NodeCapacity) {
        self.node_capacity
            .write()
            .await
            .insert(node_id.clone(), capacity);
    }

    /// Get node status
    pub async fn get_node_status(&self, node_id: &NodeId) -> Option<NodeStatus> {
        self.nodes
            .read()
            .await
            .get(node_id)
            .map(|r| r.status.clone())
    }

    /// Get node capacity
    pub async fn get_node_capacity(&self, node_id: &NodeId) -> Option<NodeCapacity> {
        self.node_capacity.read().await.get(node_id).cloned()
    }

    /// Add node to group
    pub async fn add_node_to_group(
        &self,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        let registration = nodes.get_mut(node_id).ok_or_else(|| {
            Error::Node(NodeError::NotFound {
                id: node_id.clone(),
            })
        })?;

        // Check capacity
        if registration.groups.len() >= self.config.max_groups_per_node {
            return Err(Error::Node(NodeError::AtCapacity {
                id: node_id.clone(),
            }));
        }

        registration.groups.insert(group_id);

        // Update capacity metrics
        if let Some(capacity) = self.node_capacity.write().await.get_mut(node_id) {
            capacity.group_count = registration.groups.len();
        }

        Ok(())
    }

    /// Remove node from group
    pub async fn remove_node_from_group(
        &self,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        let registration = nodes.get_mut(node_id).ok_or_else(|| {
            Error::Node(NodeError::NotFound {
                id: node_id.clone(),
            })
        })?;

        if !registration.groups.remove(&group_id) {
            return Err(Error::Node(NodeError::NotInGroup {
                node_id: node_id.clone(),
                group_id,
            }));
        }

        // Update capacity metrics
        if let Some(capacity) = self.node_capacity.write().await.get_mut(node_id) {
            capacity.group_count = registration.groups.len();
        }

        Ok(())
    }

    /// Get nodes in a group
    pub async fn get_nodes_in_group(&self, group_id: ConsensusGroupId) -> Vec<NodeId> {
        self.nodes
            .read()
            .await
            .iter()
            .filter(|(_, reg)| reg.groups.contains(&group_id))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// List all online nodes
    pub async fn list_online_nodes(&self) -> Vec<NodeId> {
        self.nodes
            .read()
            .await
            .iter()
            .filter(|(_, reg)| matches!(reg.status, NodeStatus::Online))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// List nodes with capacity
    pub async fn list_nodes_with_capacity(&self, min_capacity: f64) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        let capacities = self.node_capacity.read().await;

        nodes
            .iter()
            .filter(|(id, reg)| {
                matches!(reg.status, NodeStatus::Online)
                    && capacities
                        .get(*id)
                        .map(|c| c.capacity_score >= min_capacity)
                        .unwrap_or(false)
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get the best node for a new group assignment
    pub async fn get_best_node_for_group(&self) -> Option<NodeId> {
        let nodes = self.nodes.read().await;
        let capacities = self.node_capacity.read().await;

        nodes
            .iter()
            .filter(|(_, reg)| {
                matches!(reg.status, NodeStatus::Online)
                    && reg.groups.len() < self.config.max_groups_per_node
            })
            .map(|(id, reg)| {
                let capacity = capacities.get(id).cloned().unwrap_or_default();
                (id.clone(), capacity.capacity_score, reg.groups.len())
            })
            .max_by(|(_, score_a, groups_a), (_, score_b, groups_b)| {
                // Prefer nodes with higher capacity score and fewer groups
                score_a
                    .partial_cmp(score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| groups_b.cmp(groups_a))
            })
            .map(|(id, _, _)| id)
    }

    /// Monitor node health
    pub async fn monitor_health(&self) {
        let now = SystemTime::now();
        let mut nodes = self.nodes.write().await;

        for (node_id, registration) in nodes.iter_mut() {
            let time_since_heartbeat = now
                .duration_since(registration.last_heartbeat)
                .unwrap_or(Duration::from_secs(0));

            match &registration.status {
                NodeStatus::Online => {
                    if time_since_heartbeat > self.config.heartbeat_timeout {
                        warn!(
                            "Node {} suspected offline (no heartbeat for {:?})",
                            node_id, time_since_heartbeat
                        );
                        registration.status = NodeStatus::Suspected {
                            last_seen: registration.last_heartbeat,
                        };
                    }
                }
                NodeStatus::Suspected { last_seen } => {
                    let time_since_seen = now
                        .duration_since(*last_seen)
                        .unwrap_or(Duration::from_secs(0));

                    if time_since_seen > self.config.offline_timeout {
                        warn!("Node {} confirmed offline", node_id);
                        registration.status = NodeStatus::Offline { since: now };
                    }
                }
                _ => {}
            }
        }
    }

    /// Calculate capacity score for a node
    pub fn calculate_capacity_score(capacity: &NodeCapacity) -> f64 {
        // Simple weighted average of resource usage
        let cpu_weight = 0.3;
        let memory_weight = 0.3;
        let disk_weight = 0.2;
        let network_weight = 0.2;

        let usage_score = (capacity.cpu_usage as f64 * cpu_weight
            + capacity.memory_usage as f64 * memory_weight
            + capacity.disk_usage as f64 * disk_weight
            + capacity.network_usage as f64 * network_weight)
            / 100.0;

        // Invert so higher score means more capacity
        1.0 - usage_score
    }

    /// Set node status
    pub async fn set_node_status(
        &self,
        node_id: &NodeId,
        status: NodeStatus,
    ) -> ConsensusResult<()> {
        let mut nodes = self.nodes.write().await;

        let registration = nodes.get_mut(node_id).ok_or_else(|| {
            Error::Node(NodeError::NotFound {
                id: node_id.clone(),
            })
        })?;

        registration.status = status;
        Ok(())
    }

    /// Get failed nodes
    pub async fn get_failed_nodes(&self) -> Vec<(NodeId, SystemTime)> {
        self.nodes
            .read()
            .await
            .iter()
            .filter_map(|(id, reg)| match &reg.status {
                NodeStatus::Offline { since } => Some((id.clone(), *since)),
                _ => None,
            })
            .collect()
    }
}
