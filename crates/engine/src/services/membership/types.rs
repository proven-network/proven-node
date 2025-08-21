//! Types for membership service

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use proven_topology::{Node, NodeId};

use crate::foundation::types::{ClusterFormationState, NodeRole, NodeStatus};

/// Current view of cluster membership
#[derive(Debug, Clone)]
pub struct MembershipView {
    /// All discovered nodes and their states
    pub nodes: HashMap<NodeId, NodeMembership>,
    /// Current cluster formation state
    pub cluster_state: ClusterFormationState,
    /// Last update timestamp
    /// Last update timestamp (ms since UNIX_EPOCH)
    pub last_updated_ms: u64,
}

impl MembershipView {
    /// Create a new empty membership view
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            cluster_state: ClusterFormationState::NotFormed,
            last_updated_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Get all online nodes
    pub fn online_nodes(&self) -> Vec<&NodeMembership> {
        self.nodes
            .values()
            .filter(|n| matches!(n.status, NodeStatus::Online))
            .collect()
    }

    /// Get all nodes in a specific role
    pub fn nodes_with_role(&self, role: &NodeRole) -> Vec<&NodeMembership> {
        self.nodes
            .values()
            .filter(|n| n.roles.contains(role))
            .collect()
    }

    /// Get all members
    pub fn get_members(&self) -> Vec<(NodeId, &NodeMembership)> {
        self.nodes.iter().map(|(id, m)| (*id, m)).collect()
    }

    /// Get cluster ID (if formed)
    pub fn cluster_id(&self) -> uuid::Uuid {
        // Generate a deterministic ID based on the cluster formation
        // In a real implementation, this would be stored
        uuid::Uuid::new_v4()
    }

    /// Get coordinator node (if any)
    pub fn get_coordinator(&self) -> Option<NodeId> {
        match &self.cluster_state {
            ClusterFormationState::Forming { coordinator, .. } => Some(*coordinator),
            _ => None,
        }
    }
}

/// Information about a node's membership
#[derive(Debug, Clone)]
pub struct NodeMembership {
    pub node_id: NodeId,
    pub node_info: Node,
    pub status: NodeStatus,
    pub health: HealthInfo,
    pub roles: HashSet<NodeRole>,
    /// When the node was last seen (ms since UNIX_EPOCH)
    pub last_seen_ms: u64,
}

/// Health information for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    /// Timestamp of last health check (ms since UNIX_EPOCH)
    pub last_check_ms: u64,
    pub consecutive_failures: u32,
    pub latency_ms: Option<u32>,
    pub load: Option<LoadInfo>,
}

impl HealthInfo {
    pub fn new() -> Self {
        Self {
            last_check_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            consecutive_failures: 0,
            latency_ms: None,
            load: None,
        }
    }
}

/// Load information for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadInfo {
    pub cpu_percent: f32,
    pub memory_percent: f32,
    pub connection_count: u32,
}
