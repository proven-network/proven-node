//! Local consensus management for stream data operations
//!
//! This module provides the local consensus layer that manages
//! individual stream data within allocated consensus groups.

/// Group discovery for local consensus groups
pub mod group_discovery;
/// Group storage for Raft consensus operations
pub mod group_storage;
/// Legacy types for migration compatibility
pub mod legacy_types;
/// Local consensus manager for managing local consensus groups
pub mod local_manager;
/// Network factory for creating local consensus groups
pub mod network_factory;
/// State command pattern for local operations
pub mod state_command;
/// State machine implementation
pub mod state_machine;
/// Stream storage for stream data operations
pub mod stream_storage;

pub use local_manager::{LocalConsensusManager, MigrationState as NodeMigrationState};
pub use state_machine::StorageBackedLocalState;
// Re-export legacy types for migration compatibility
pub use legacy_types::{
    LocalStateMetrics, MessageData, PendingOperation, PendingOperationType, StreamData,
    StreamMetrics,
};
// Re-export LocalStreamOperation from operations module
pub use crate::operations::LocalStreamOperation;

use crate::allocation::ConsensusGroupId;
use crate::node::Node;
use crate::node_id::NodeId;

use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

openraft::declare_raft_types!(
    /// Types for local consensus groups
    pub LocalTypeConfig:
        D = LocalRequest,
        R = LocalResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<LocalTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Request for local consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalRequest {
    /// The operation to perform
    pub operation: LocalStreamOperation,
}

/// Response from local consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful (for stream operations)
    pub sequence: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Checkpoint data (for checkpoint operations)
    pub checkpoint_data: Option<bytes::Bytes>,
}

/// Migration state for stream migrations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MigrationState {
    /// Preparing for migration
    Preparing,
    /// Transferring initial data snapshot
    Transferring,
    /// Syncing incremental updates to minimize downtime
    Syncing,
    /// Switching traffic to target group
    Switching,
    /// Completing migration and cleanup
    Completing,
    /// Migration completed successfully
    Completed,
    /// Migration failed
    Failed,
}

/// Migration state for a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMigrationState {
    /// Stream being migrated
    pub stream_name: String,
    /// Source consensus group
    pub source_group: ConsensusGroupId,
    /// Target consensus group
    pub target_group: ConsensusGroupId,
    /// Migration started at
    pub started_at: u64,
    /// Last sequence number migrated
    pub last_migrated_seq: u64,
    /// Migration phase
    pub phase: MigrationPhase,
}

/// Phases of stream migration
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum MigrationPhase {
    /// Initial snapshot being taken
    Snapshot,
    /// Catching up with new messages
    CatchUp,
    /// Final switch over
    Switch,
    /// Migration complete
    Complete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_state_transitions() {
        // Test that migration states transition correctly
        let states = vec![
            MigrationState::Preparing,
            MigrationState::Transferring,
            MigrationState::Switching,
            MigrationState::Completing,
            MigrationState::Completed,
        ];

        // Verify each state is distinct
        for (i, state1) in states.iter().enumerate() {
            for (j, state2) in states.iter().enumerate() {
                if i == j {
                    assert_eq!(state1, state2);
                } else {
                    assert_ne!(state1, state2);
                }
            }
        }

        // Failed state should be distinct from all others
        let failed = MigrationState::Failed;
        for state in &states {
            assert_ne!(state, &failed);
        }
    }
}
