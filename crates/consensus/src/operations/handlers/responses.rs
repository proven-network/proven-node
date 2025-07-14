//! Type-safe responses for operation handlers
//!
//! Each operation type has its own response variant, providing strong typing
//! and clear semantics for what each operation returns.

use crate::ConsensusGroupId;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};

/// Response from any global operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalOperationResponse {
    /// Response from stream management operations
    StreamManagement(StreamManagementResponse),
    /// Response from group operations
    Group(GroupOperationResponse),
    /// Response from node operations
    Node(NodeOperationResponse),
    /// Response from routing operations
    Routing(RoutingOperationResponse),
}

impl GlobalOperationResponse {
    /// Whether the operation succeeded
    pub fn is_success(&self) -> bool {
        match self {
            Self::StreamManagement(r) => r.is_success(),
            Self::Group(r) => r.is_success(),
            Self::Node(r) => r.is_success(),
            Self::Routing(r) => r.is_success(),
        }
    }

    /// Get the sequence number if applicable
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::StreamManagement(r) => r.sequence(),
            Self::Group(r) => r.sequence(),
            Self::Node(r) => r.sequence(),
            Self::Routing(r) => r.sequence(),
        }
    }

    /// Get any error message
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::StreamManagement(r) => r.error(),
            Self::Group(r) => r.error(),
            Self::Node(r) => r.error(),
            Self::Routing(r) => r.error(),
        }
    }
}

/// Response from stream management operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamManagementResponse {
    /// Stream was created successfully
    Created {
        /// The sequence number of the create operation
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The consensus group it was created in
        group_id: ConsensusGroupId,
    },
    /// Stream was deleted successfully
    Deleted {
        /// The sequence number of the delete operation
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Number of messages that were in the stream
        message_count: u64,
    },
    /// Stream was reallocated to a different group
    Reallocated {
        /// The sequence number of the reallocation
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Previous group
        from_group: ConsensusGroupId,
        /// New group
        to_group: ConsensusGroupId,
    },
    /// Stream configuration was updated
    ConfigUpdated {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// What configuration changed
        changes: Vec<String>,
    },
    /// Stream migration started
    MigrationStarted {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Migration ID for tracking
        migration_id: String,
    },
    /// Operation failed
    Failed {
        /// What operation failed
        operation: String,
        /// Why it failed
        reason: String,
    },
}

impl StreamManagementResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Created { sequence, .. } => Some(*sequence),
            Self::Deleted { sequence, .. } => Some(*sequence),
            Self::Reallocated { sequence, .. } => Some(*sequence),
            Self::ConfigUpdated { sequence, .. } => Some(*sequence),
            Self::MigrationStarted { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from group operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupOperationResponse {
    /// Group was created
    Created {
        /// The sequence number
        sequence: u64,
        /// The group ID
        group_id: ConsensusGroupId,
        /// Initial members
        members: Vec<NodeId>,
    },
    /// Group was deleted
    Deleted {
        /// The sequence number
        sequence: u64,
        /// The group ID
        group_id: ConsensusGroupId,
    },
    /// Group members were updated
    MembersUpdated {
        /// The sequence number
        sequence: u64,
        /// The group ID
        group_id: ConsensusGroupId,
        /// Members added
        added: Vec<NodeId>,
        /// Members removed
        removed: Vec<NodeId>,
    },
    /// Operation failed
    Failed {
        /// What operation failed
        operation: String,
        /// Why it failed
        reason: String,
    },
}

impl GroupOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Created { sequence, .. } => Some(*sequence),
            Self::Deleted { sequence, .. } => Some(*sequence),
            Self::MembersUpdated { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from node operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeOperationResponse {
    /// Node was assigned to a group
    AssignedToGroup {
        /// The sequence number
        sequence: u64,
        /// The node ID
        node_id: NodeId,
        /// The group ID
        group_id: ConsensusGroupId,
        /// Total groups this node is now in
        total_groups: usize,
    },
    /// Node was removed from a group
    RemovedFromGroup {
        /// The sequence number
        sequence: u64,
        /// The node ID
        node_id: NodeId,
        /// The group ID
        group_id: ConsensusGroupId,
        /// Remaining groups this node is in
        remaining_groups: usize,
    },
    /// Node was transferred between groups
    Transferred {
        /// The sequence number
        sequence: u64,
        /// The node ID
        node_id: NodeId,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
    },
    /// Operation failed
    Failed {
        /// What operation failed
        operation: String,
        /// Why it failed
        reason: String,
    },
}

impl NodeOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::AssignedToGroup { sequence, .. } => Some(*sequence),
            Self::RemovedFromGroup { sequence, .. } => Some(*sequence),
            Self::Transferred { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from routing operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingOperationResponse {
    /// Stream was subscribed to a subject
    Subscribed {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The subject pattern
        subject_pattern: String,
        /// Total subscriptions for this stream
        total_subscriptions: usize,
    },
    /// Stream was unsubscribed from a subject
    Unsubscribed {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The subject pattern
        subject_pattern: String,
        /// Remaining subscriptions for this stream
        remaining_subscriptions: usize,
    },
    /// Routing configuration was updated
    ConfigUpdated {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// What changed
        changes: Vec<String>,
    },
    /// Operation failed
    Failed {
        /// What operation failed
        operation: String,
        /// Why it failed
        reason: String,
    },
}

impl RoutingOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Subscribed { sequence, .. } => Some(*sequence),
            Self::Unsubscribed { sequence, .. } => Some(*sequence),
            Self::ConfigUpdated { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

// Implement conversions from specific responses to OperationResponse
impl From<StreamManagementResponse> for GlobalOperationResponse {
    fn from(resp: StreamManagementResponse) -> Self {
        GlobalOperationResponse::StreamManagement(resp)
    }
}

impl From<GroupOperationResponse> for GlobalOperationResponse {
    fn from(resp: GroupOperationResponse) -> Self {
        GlobalOperationResponse::Group(resp)
    }
}

impl From<NodeOperationResponse> for GlobalOperationResponse {
    fn from(resp: NodeOperationResponse) -> Self {
        GlobalOperationResponse::Node(resp)
    }
}

impl From<RoutingOperationResponse> for GlobalOperationResponse {
    fn from(resp: RoutingOperationResponse) -> Self {
        GlobalOperationResponse::Routing(resp)
    }
}

/// Convert from legacy GlobalResponse to OperationResponse
/// This is a compatibility layer that will be removed once all code is migrated
impl From<crate::core::global::GlobalResponse> for GlobalOperationResponse {
    fn from(old: crate::core::global::GlobalResponse) -> Self {
        if old.success {
            // We don't have enough context to create a specific response,
            // so we create a generic success response
            // This is why we need to migrate away from GlobalResponse
            GlobalOperationResponse::StreamManagement(StreamManagementResponse::ConfigUpdated {
                sequence: old.sequence,
                stream_name: String::new(),
                changes: vec!["Legacy operation completed".to_string()],
            })
        } else {
            GlobalOperationResponse::StreamManagement(StreamManagementResponse::Failed {
                operation: "LegacyOperation".to_string(),
                reason: old.error.unwrap_or_else(|| "Unknown error".to_string()),
            })
        }
    }
}

/// Convert OperationResponse back to GlobalResponse for compatibility
impl From<GlobalOperationResponse> for crate::core::global::GlobalResponse {
    fn from(resp: GlobalOperationResponse) -> Self {
        crate::core::global::GlobalResponse {
            success: resp.is_success(),
            sequence: resp.sequence().unwrap_or(0),
            error: resp.error().map(|s| s.to_string()),
        }
    }
}
