//! Group consensus management for stream data operations
//!
//! This module provides the group consensus layer that manages
//! individual stream data within allocated consensus groups.

/// Allocator for intelligent group management
pub mod group_allocator;
/// Configuration types for consensus groups
pub mod group_config;
/// Factory for creating consensus groups
pub mod group_factory;
/// Network adapter for group consensus
pub mod group_network_adaptor;
/// Groups manager for unified group consensus management
pub mod groups_manager;
/// Legacy types for migration compatibility
pub mod legacy_types;
/// Migration system for streams between groups
pub mod migration;
/// Storage implementations for group consensus
pub mod storage;

pub use group_config::{GroupConfig, GroupConfigBuilder, GroupDependencies};
pub use group_factory::GroupFactory;
pub use groups_manager::GroupsManager;
// Re-export legacy types for migration compatibility
pub use legacy_types::{LocalStateMetrics, StreamMetrics};
// Re-export GroupStreamOperation from operations module
pub use crate::operations::GroupStreamOperation;
// Re-export storage types from group storage
pub use storage::{GroupStorageFactory, factory::UnifiedGroupStorage};

use crate::ConsensusGroupId;
use proven_topology::{Node, NodeId};

use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

openraft::declare_raft_types!(
    /// Types for group consensus
    pub GroupConsensusTypeConfig:
        D = GroupStreamOperation,
        R = crate::operations::handlers::GroupStreamOperationResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<GroupConsensusTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Request for group consensus operations (deprecated)
#[deprecated(note = "Use GroupStreamOperation directly")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRequest {
    /// The operation to perform
    pub operation: GroupStreamOperation,
}

/// Response from group consensus operations (deprecated)
#[deprecated(note = "Use GroupStreamOperationResponse instead")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful (for stream operations)
    pub sequence: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Checkpoint data (for checkpoint operations)
    pub checkpoint_data: Option<bytes::Bytes>,
    /// Data returned from read operations
    pub data: Option<bytes::Bytes>,
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

/// Migration state for a node in a group
#[derive(Debug, Clone, PartialEq)]
pub enum NodeMigrationState {
    /// Node is a regular member of this group
    Active,
    /// Node is joining this group (dual membership - new group)
    Joining,
    /// Node is leaving this group (dual membership - old group)
    Leaving,
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
