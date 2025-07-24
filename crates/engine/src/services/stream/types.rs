//! Core stream types

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

use proven_storage::LogIndex;
use proven_topology::NodeId;

use crate::foundation::types::ConsensusGroupId;

/// Stream name type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamName(String);

impl StreamName {
    /// Create a new stream name
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for StreamName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for StreamName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

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

/// Stream health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamHealth {
    /// Stream is healthy
    Healthy,
    /// Stream is degraded (some replicas offline)
    Degraded,
    /// Stream is offline
    Offline,
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

/// Stored message with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Sequence number
    pub sequence: LogIndex,
    /// Message data
    pub data: MessageData,
    /// Timestamp
    pub timestamp: u64,
}

/// Serialize a StoredMessage to bytes
pub fn serialize_stored_message(msg: &StoredMessage) -> Result<Bytes, String> {
    serde_json::to_vec(msg)
        .map(Bytes::from)
        .map_err(|e| format!("Failed to serialize message: {e}"))
}

/// Deserialize a StoredMessage from bytes
pub fn deserialize_stored_message(bytes: Bytes) -> Result<StoredMessage, String> {
    serde_json::from_slice(&bytes).map_err(|e| format!("Failed to deserialize message: {e}"))
}
