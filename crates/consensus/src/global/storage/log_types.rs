//! Global consensus-specific log types
//!
//! This module defines the log entry types and metadata specific to
//! global consensus (Raft) operations.

use crate::storage::{LogEntry, StorageNamespace};
use serde::{Deserialize, Serialize};

/// Type alias for global consensus log entries
pub type GlobalLogEntry = LogEntry<GlobalLogMetadata>;

/// Metadata specific to global Raft log entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalLogMetadata {
    /// The term when this entry was created
    pub term: u64,
    /// The type of Raft entry
    pub entry_type: GlobalEntryType,
}

/// Types of entries in the global Raft log
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GlobalEntryType {
    /// Normal log entry containing a command
    Normal,
    /// Configuration change entry
    Configuration,
    /// Snapshot pointer entry
    Snapshot,
    /// Blank entry (for leader election)
    Blank,
}

/// Namespace constants for global consensus storage
pub mod namespaces {
    /// Namespace for Raft log entries
    pub const LOGS: &str = "logs";

    /// Namespace for metadata
    pub const METADATA: &str = "metadata";

    /// Namespace for snapshots
    pub const SNAPSHOTS: &str = "snapshots";
}

/// Helper functions for creating global storage namespaces
pub mod namespace_helpers {
    use super::*;

    /// Create a namespace for global Raft logs
    pub fn global_logs_namespace() -> StorageNamespace {
        StorageNamespace::new(namespaces::LOGS.to_string())
    }

    /// Create a namespace for global metadata
    pub fn global_metadata_namespace() -> StorageNamespace {
        StorageNamespace::new(namespaces::METADATA.to_string())
    }

    /// Create a namespace for global snapshots
    pub fn global_snapshots_namespace() -> StorageNamespace {
        StorageNamespace::new(namespaces::SNAPSHOTS.to_string())
    }
}
