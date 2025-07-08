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

use crate::node::Node;
use crate::node_id::NodeId;
use crate::operations::LocalStreamOperation;
use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

pub use local_manager::LocalConsensusManager;
pub use state_machine::LocalState;

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
}
