//! Global consensus-specific log types
//!
//! This module defines the log entry types and metadata specific to
//! global consensus (Raft) operations.

use crate::storage_backends::LogEntry;
use serde::{Deserialize, Serialize};

/// Type alias for global consensus log entries
pub type GlobalLogEntry = LogEntry<GlobalLogMetadata>;

/// Metadata specific to global Raft log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalLogMetadata {
    /// The term when this entry was created
    pub term: u64,
    /// The leader node ID that created this entry
    pub leader_node_id: proven_topology::NodeId,
    /// The type of Raft entry
    pub entry_type: GlobalEntryType,
}

/// Types of entries in the global Raft log
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalEntryType {
    /// Normal log entry containing a command
    Normal,
    /// Membership change entry
    Membership,
    /// Blank entry (for leader election)
    Blank,
}
