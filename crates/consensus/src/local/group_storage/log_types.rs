//! Log types for local Raft storage
//!
//! This module defines metadata types used with the LogStorage trait
//! for local consensus groups.

use crate::node_id::NodeId;
use serde::{Deserialize, Serialize};

/// Metadata for local Raft log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalLogMetadata {
    /// The term when this entry was created
    pub term: u64,
    /// The leader node ID that created this entry
    pub leader_node_id: NodeId,
    /// The type of entry
    pub entry_type: LocalEntryType,
    /// Consensus group ID this entry belongs to
    pub group_id: crate::allocation::ConsensusGroupId,
}

/// Types of entries in the local Raft log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LocalEntryType {
    /// Stream operation (publish, delete, etc.)
    StreamOperation {
        /// Stream ID being operated on
        stream_id: String,
        /// Type of operation
        operation: StreamOperationType,
    },
    /// Stream management (create, remove, etc.)
    StreamManagement,
    /// Membership change
    MembershipChange,
    /// Migration-related entry
    Migration,
    /// Empty entry (heartbeat)
    Empty,
}

/// Types of stream operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOperationType {
    /// Publish message
    Publish,
    /// Delete message
    Delete,
    /// Rollup operation
    Rollup,
    /// Pause stream
    Pause,
    /// Resume stream
    Resume,
}
