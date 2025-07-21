//! Types for membership service

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use proven_topology::{Node, NodeId};

use crate::foundation::types::ConsensusGroupId;

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

/// Node status in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,
    /// Node is online and healthy
    Online,
    /// Node is temporarily unreachable
    Unreachable { since_ms: u64 },
    /// Node is confirmed offline
    Offline { since_ms: u64 },
    /// Node is in maintenance mode
    Maintenance,
}

impl NodeStatus {
    pub fn is_available(&self) -> bool {
        matches!(self, NodeStatus::Online)
    }
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

/// Roles a node can have in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeRole {
    /// Leader of global consensus
    GlobalConsensusLeader,
    /// Member of global consensus
    GlobalConsensusMember,
    /// Leader of a specific group
    GroupLeader(ConsensusGroupId),
    /// Member of a specific group
    GroupMember(ConsensusGroupId),
}

/// State of cluster formation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterFormationState {
    /// Cluster not yet formed
    NotFormed,
    /// Currently discovering nodes
    Discovering { round: u32, started_at_ms: u64 },
    /// Currently forming cluster
    Forming {
        coordinator: NodeId,
        formation_id: uuid::Uuid,
        proposed_members: Vec<NodeId>,
    },
    /// Cluster is active
    Active {
        members: Vec<NodeId>,
        formed_at_ms: u64,
    },
}
