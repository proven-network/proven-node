//! Global consensus components
//!
//! This module contains the global consensus manager and related components
//! including state machine, storage implementations, and snapshot functionality.

pub mod global_manager;
pub mod global_state;
pub mod snapshot;
pub mod state_command;
pub mod state_machine;
pub mod storage;

use crate::node::Node;
use crate::node_id::NodeId;
use crate::operations::GlobalOperation;

use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

// Re-export main types
pub use global_manager::{GlobalManager, PendingRequest};
pub use global_state::{ConsensusGroupInfo, GlobalState, MessageData, StreamData};
pub use snapshot::SnapshotData;
pub use state_command::{CommandFactory, CommandProcessor, StateCommand};
pub use state_machine::GlobalStateMachine;
pub use storage::ConsensusStorage;

// Import stream-related types from local stream storage
pub use crate::local::stream_storage::traits::{RetentionPolicy, StorageType, StreamConfig};

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub GlobalTypeConfig:
        D = GlobalRequest,
        R = GlobalResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<GlobalTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Request for consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRequest {
    /// The operation to perform
    pub operation: GlobalOperation,
}

/// Response from consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful
    pub sequence: u64,
    /// Error message if failed
    pub error: Option<String>,
}

/// Source information for PubSub messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessageSource {
    /// Node that published the message
    pub node_id: Option<NodeId>,
    /// Timestamp when received
    pub timestamp_secs: u64,
}
