// This file contained the old LocalState implementation
// which has been replaced by StorageBackedLocalState.
//
// The old implementation used in-memory data structures
// while the new implementation uses the storage abstraction layer.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Legacy stream data type - kept for compatibility with migration checkpoints
/// New implementations should use StorageBackedLocalState directly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    /// Messages in the stream
    pub messages: std::collections::BTreeMap<u64, MessageData>,
    /// Last sequence number
    pub last_seq: u64,
    /// Whether the stream is paused for migration
    pub is_paused: bool,
    /// Pending operations while paused (for atomic resume)
    pub pending_operations: Vec<PendingOperation>,
    /// Pause timestamp for migration coordination
    pub paused_at: Option<u64>,
}

/// Legacy pending operation type - kept for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOperation {
    /// Operation type
    pub operation_type: PendingOperationType,
    /// Data for the operation
    pub data: bytes::Bytes,
    /// Metadata
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp when operation was attempted
    pub timestamp: u64,
}

/// Legacy pending operation types - kept for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PendingOperationType {
    /// Publish message operation
    Publish,
    /// Rollup operation
    Rollup,
    /// Delete message operation
    Delete {
        /// Sequence number of message to delete
        sequence: u64,
    },
}

/// Legacy message data type - kept for compatibility with migration checkpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    /// Sequence number
    pub sequence: u64,
    /// Message content
    pub data: bytes::Bytes,
    /// Timestamp
    pub timestamp: u64,
    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,
}

// LocalState has been replaced by StorageBackedLocalState
// Only keeping legacy types for compatibility with existing migration checkpoints

/// Legacy metrics for a local state machine - kept for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStateMetrics {
    /// Group ID
    pub group_id: Option<crate::allocation::ConsensusGroupId>,
    /// Number of streams
    pub stream_count: u32,
    /// Total messages
    pub total_messages: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Current message rate (messages per second)
    pub message_rate: f64,
    /// Per-stream metrics
    pub streams: HashMap<String, StreamMetrics>,
}

/// Legacy metrics for a single stream - kept for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetrics {
    /// Number of messages
    pub message_count: u64,
    /// Last sequence number
    pub last_sequence: u64,
    /// Total bytes
    pub total_bytes: u64,
}

/// Legacy snapshot of a local state machine - kept for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStateSnapshot {
    /// Group ID
    pub group_id: Option<crate::allocation::ConsensusGroupId>,
    /// All streams and their data
    pub streams: HashMap<String, StreamData>,
    /// Total messages
    pub total_messages: u64,
    /// Total bytes
    pub total_bytes: u64,
}
