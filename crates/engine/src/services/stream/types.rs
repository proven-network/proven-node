//! Core stream types

use serde::{Deserialize, Serialize};

use proven_storage::LogIndex;
use proven_topology::NodeId;

use crate::foundation::StoredMessage;
use crate::foundation::types::{ConsensusGroupId, StreamHealth, StreamName};

/// Message data type alias for the foundation Message type
pub type MessageData = crate::foundation::Message;

/// Stream metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Stream name
    pub name: StreamName,
    /// Group that owns this stream
    pub group_id: ConsensusGroupId,
    /// Creation timestamp
    pub created_at: u64,
    /// Current leader node
    pub leader: Option<NodeId>,
    /// Replica nodes
    pub replicas: Vec<NodeId>,
}

/// Stream state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamState {
    /// Stream is being created
    Creating,
    /// Stream is active
    Active,
    /// Stream is paused
    Paused,
    /// Stream is migrating
    Migrating,
    /// Stream is being deleted
    Deleting,
}

/// Stream statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStats {
    /// Metadata
    pub metadata: StreamMetadata,
    /// Last committed sequence number
    pub last_seq: u64,
    /// Number of messages
    pub message_count: LogIndex,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Stream health status
    pub health: StreamHealth,
    /// Stream state
    pub state: StreamState,
}

/// Stream append result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResult {
    /// Assigned sequence number
    pub sequence: LogIndex,
    /// Timestamp
    pub timestamp: u64,
}

/// Stream read result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    /// Messages read
    pub messages: Vec<StoredMessage>,
    /// Next sequence to read
    pub next_seq: LogIndex,
    /// Whether more messages are available
    pub has_more: bool,
}
