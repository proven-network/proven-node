//! Core types for the routing table

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};

/// Routing decision for an operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RoutingDecision {
    /// Route to global consensus
    Global,
    /// Route to specific group
    Group(ConsensusGroupId),
    /// Route to local processing
    Local,
    /// Reject the operation
    Reject {
        /// Reason for rejection
        reason: String,
    },
}

/// Stream routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRoute {
    /// Stream name
    pub stream_name: String,
    /// Assigned group
    pub group_id: ConsensusGroupId,
    /// Assignment timestamp
    pub assigned_at: std::time::SystemTime,
    /// Is route active
    pub is_active: bool,
}

/// Group routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRoute {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Group members
    pub members: Vec<NodeId>,
    /// Group leader (if known)
    pub leader: Option<NodeId>,
    /// Number of streams assigned to this group
    pub stream_count: usize,
}

/// Group location relative to the current node
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GroupLocation {
    /// Group exists on this node
    Local,
    /// Group exists on remote nodes only
    Remote,
}

/// Routing errors
#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    /// Group not found
    #[error("Group not found: {0:?}")]
    GroupNotFound(ConsensusGroupId),

    /// Stream not found
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// No available groups
    #[error("No available groups for routing")]
    NoAvailableGroups,

    /// Internal error
    #[error("Internal routing error: {0}")]
    Internal(String),
}
