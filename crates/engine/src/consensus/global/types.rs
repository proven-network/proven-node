//! Types for global consensus layer

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::foundation::GroupInfo;
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::{StreamConfig, StreamName};
use proven_topology::NodeId;

/// Global consensus request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalRequest {
    /// Create a new stream
    CreateStream {
        /// Stream name
        name: StreamName,
        /// Stream configuration
        config: StreamConfig,
        /// Target group
        group_id: ConsensusGroupId,
    },
    /// Delete a stream
    DeleteStream {
        /// Stream name
        name: StreamName,
    },
    /// Update stream configuration
    UpdateStreamConfig {
        /// Stream name
        name: StreamName,
        /// New configuration
        config: StreamConfig,
    },
    /// Create a consensus group
    CreateGroup {
        /// Group info
        info: GroupInfo,
    },
    /// Dissolve a consensus group
    DissolveGroup {
        /// Group ID
        id: ConsensusGroupId,
    },
    /// Add node to cluster
    AddNode {
        /// Node ID
        node_id: NodeId,
        /// Node metadata
        metadata: HashMap<String, String>,
    },
    /// Remove node from cluster
    RemoveNode {
        /// Node ID
        node_id: NodeId,
    },
    /// Reassign stream to different group
    ReassignStream {
        /// Stream name
        name: StreamName,
        /// New group
        to_group: ConsensusGroupId,
    },
}

/// Global consensus response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalResponse {
    /// Operation succeeded
    Success,
    /// Stream created
    StreamCreated {
        /// Stream name
        name: StreamName,
        /// Assigned group
        group_id: ConsensusGroupId,
    },
    /// Stream deleted
    StreamDeleted {
        /// Stream name
        name: StreamName,
    },
    /// Group created
    GroupCreated {
        /// Group ID
        id: ConsensusGroupId,
        /// Group info
        group_info: GroupInfo,
    },
    /// Group dissolved
    GroupDissolved {
        /// Group ID
        id: ConsensusGroupId,
    },
    /// Node added
    NodeAdded {
        /// Node ID
        node_id: NodeId,
    },
    /// Node removed
    NodeRemoved {
        /// Node ID
        node_id: NodeId,
    },
    /// Error response
    Error {
        /// Error message
        message: String,
    },
    /// Stream already exists
    StreamAlreadyExists {
        /// Stream name
        name: StreamName,
        /// Current group assignment
        group_id: ConsensusGroupId,
    },
}

impl GlobalResponse {
    /// Create an error response
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    /// Create a success response
    pub fn success() -> Self {
        Self::Success
    }
}
