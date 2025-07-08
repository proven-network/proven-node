//! Local consensus management for stream data operations
//!
//! This module provides the local consensus layer that manages
//! individual stream data within allocated consensus groups.

/// Group discovery for local consensus groups
pub mod group_discovery;
/// Local consensus manager for managing local consensus groups
pub mod local_manager;
/// Network factory for creating local consensus groups
pub mod network_factory;
/// Network registry for local groups
pub mod state_machine;
/// Storage for local consensus groups
pub mod storage;

pub use local_manager::{LocalConsensusManager, MigrationState as NodeMigrationState};
pub use state_machine::LocalState;

use crate::allocation::ConsensusGroupId;
use crate::global::PubSubMessageSource;
use crate::node::Node;
use crate::node_id::NodeId;

use bytes::Bytes;
use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// Local stream operations handled by local consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LocalStreamOperation {
    /// Publish a message to a stream
    PublishToStream {
        /// Stream name to publish to
        stream: String,
        /// Message data
        data: Bytes,
        /// Optional metadata
        metadata: Option<HashMap<String, String>>,
    },

    /// Publish multiple messages to a stream
    PublishBatchToStream {
        /// Stream name to publish to
        stream: String,
        /// Messages to publish
        messages: Vec<Bytes>,
    },

    /// Rollup operation on a stream
    RollupStream {
        /// Stream name to rollup
        stream: String,
        /// Message data
        data: Bytes,
        /// Expected sequence number
        expected_seq: u64,
    },

    /// Delete a message from a stream
    DeleteFromStream {
        /// Stream name to delete from
        stream: String,
        /// Sequence number of the message to delete
        sequence: u64,
    },

    /// Publish from PubSub to a stream
    PublishFromPubSub {
        /// Stream name to publish to
        stream_name: String,
        /// Subject that triggered this
        subject: String,
        /// Message data
        data: Bytes,
        /// Source information
        source: PubSubMessageSource,
    },

    /// Create a stream for migration
    CreateStreamForMigration {
        /// Stream name to create
        stream_name: String,
        /// Source consensus group
        source_group: ConsensusGroupId,
    },

    /// Get a checkpoint of stream data for migration
    GetStreamCheckpoint {
        /// Stream name to checkpoint
        stream_name: String,
    },

    /// Get an incremental checkpoint of stream data since a sequence number
    GetIncrementalCheckpoint {
        /// Stream name to checkpoint
        stream_name: String,
        /// Only include messages after this sequence number
        since_sequence: u64,
    },

    /// Apply a migration checkpoint
    ApplyMigrationCheckpoint {
        /// Checkpoint data
        checkpoint: Bytes,
    },

    /// Apply an incremental migration checkpoint (merges with existing data)
    ApplyIncrementalCheckpoint {
        /// Checkpoint data
        checkpoint: Bytes,
    },

    /// Pause stream for migration
    PauseStream {
        /// Stream name to pause
        stream_name: String,
    },

    /// Resume stream after migration
    ResumeStream {
        /// Stream name to resume
        stream_name: String,
    },

    /// Remove stream after migration
    RemoveStream {
        /// Stream name to remove
        stream_name: String,
    },

    /// Get metrics for the local state machine
    GetMetrics,

    /// Clean up old pending operations
    CleanupPendingOperations {
        /// Maximum age of pending operations to keep (in seconds)
        max_age_secs: u64,
    },
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
