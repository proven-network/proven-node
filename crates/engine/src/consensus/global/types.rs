//! Types for global consensus layer

use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::types::{ConsensusGroupId, StreamName};
use crate::foundation::{GroupInfo, Message, StreamConfig};
use proven_topology::NodeId;

/// Global consensus request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalRequest {
    /// Create a new stream
    CreateStream {
        /// Stream name
        stream_name: StreamName,
        /// Stream configuration
        config: StreamConfig,
        /// Placement
        placement: StreamPlacement,
    },
    /// Append messages to a global stream
    AppendToGlobalStream {
        /// Stream name
        stream_name: StreamName,
        /// Messages to append
        messages: Vec<Message>,
        /// Timestamp
        timestamp: u64,
    },
    /// Delete a stream
    DeleteStream {
        /// Stream name
        stream_name: StreamName,
    },
    /// Update stream configuration
    UpdateStreamConfig {
        /// Stream name
        stream_name: StreamName,
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
    AddNodeToGroup {
        /// Node ID
        node_id: NodeId,
        /// Node metadata
        metadata: HashMap<String, String>,
    },
    /// Remove node from cluster
    RemoveNodeFromGroup {
        /// Node ID
        node_id: NodeId,
    },
    /// Reassign stream to different placement
    ReassignStream {
        /// Stream name
        stream_name: StreamName,
        /// New placement
        new_placement: StreamPlacement,
    },
    /// Trim a global stream
    TrimGlobalStream {
        /// Stream name
        stream_name: StreamName,
        /// Trim up to this sequence
        up_to_seq: LogIndex,
    },
    /// Delete a message from a global stream
    DeleteFromGlobalStream {
        /// Stream name
        stream_name: StreamName,
        /// Sequence number to delete
        sequence: LogIndex,
    },
}

impl fmt::Display for GlobalRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateStream { stream_name, .. } => {
                write!(f, "CreateStream({})", stream_name)
            }
            Self::AppendToGlobalStream {
                stream_name,
                messages,
                ..
            } => {
                write!(
                    f,
                    "AppendToGlobalStream({}, {} messages)",
                    stream_name,
                    messages.len()
                )
            }
            Self::DeleteStream { stream_name } => {
                write!(f, "DeleteStream({})", stream_name)
            }
            Self::UpdateStreamConfig { stream_name, .. } => {
                write!(f, "UpdateStreamConfig({})", stream_name)
            }
            Self::CreateGroup { info } => {
                write!(f, "CreateGroup({})", info.id)
            }
            Self::DissolveGroup { id } => {
                write!(f, "DissolveGroup({})", id)
            }
            Self::AddNodeToGroup { node_id, .. } => {
                write!(f, "AddNodeToGroup({})", node_id)
            }
            Self::RemoveNodeFromGroup { node_id } => {
                write!(f, "RemoveNodeFromGroup({})", node_id)
            }
            Self::ReassignStream { stream_name, .. } => {
                write!(f, "ReassignStream({})", stream_name)
            }
            Self::TrimGlobalStream {
                stream_name,
                up_to_seq,
            } => {
                write!(f, "TrimGlobalStream({}, up_to: {})", stream_name, up_to_seq)
            }
            Self::DeleteFromGlobalStream {
                stream_name,
                sequence,
            } => {
                write!(
                    f,
                    "DeleteFromGlobalStream({}, seq: {})",
                    stream_name, sequence
                )
            }
        }
    }
}

/// Global consensus response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalResponse {
    /// Operation succeeded
    Success,
    /// Stream created
    StreamCreated {
        /// Stream name
        stream_name: StreamName,
        /// Placement
        placement: StreamPlacement,
    },
    /// Stream deleted
    StreamDeleted {
        /// Stream name
        stream_name: StreamName,
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
        stream_name: StreamName,
        /// Current placement
        placement: StreamPlacement,
    },
    /// Messaged appended to global stream
    Appended {
        /// Stream name
        stream_name: StreamName,
        /// Assigned sequence number
        sequence: LogIndex,
        /// Pre-serialized entries (not serialized, only for in-memory passing)
        #[serde(skip)]
        entries: Option<Arc<Vec<bytes::Bytes>>>,
    },
    /// Stream reassigned to different placement
    StreamReassigned {
        /// Stream name
        stream_name: StreamName,
        /// Previous placement
        old_placement: StreamPlacement,
        /// New placement
        new_placement: StreamPlacement,
    },
    /// Global stream trimmed
    GlobalStreamTrimmed {
        /// Stream name
        stream_name: StreamName,
        /// New start sequence
        new_start_seq: LogIndex,
    },
    /// Message deleted from global stream
    GlobalStreamMessageDeleted {
        /// Stream name
        stream_name: StreamName,
        /// Deleted sequence number
        sequence: LogIndex,
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
