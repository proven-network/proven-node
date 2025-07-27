//! Node and membership related types

use serde::{Deserialize, Serialize};

use crate::foundation::types::ConsensusGroupId;

/// Node status in the cluster
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,
    /// Node is online and healthy
    Online,
    /// Node is temporarily unreachable
    Unreachable {
        /// Time since the node became unreachable
        since_ms: u64,
    },
    /// Node is confirmed offline
    Offline {
        /// Time since the node became offline
        since_ms: u64,
    },
    /// Node is in maintenance mode
    Maintenance,
}

impl NodeStatus {
    /// Check if the node is available
    pub fn is_available(&self) -> bool {
        matches!(self, NodeStatus::Online)
    }
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
    Discovering {
        /// Round number
        round: u32,
        /// Time the discovery started
        started_at_ms: u64,
    },
    /// Currently forming cluster
    Forming {
        /// Coordinator node ID
        coordinator: proven_topology::NodeId,
        /// Formation ID
        formation_id: uuid::Uuid,
        /// Proposed members
        proposed_members: Vec<proven_topology::NodeId>,
    },
    /// Cluster is active
    Active {
        /// Members
        members: Vec<proven_topology::NodeId>,
        /// Time the cluster was formed
        formed_at_ms: u64,
    },
}
