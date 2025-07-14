//! Global consensus components
//!
//! This module contains the global consensus manager and related components
//! including state machine, storage implementations, and snapshot functionality.

pub mod global_network_adaptor;
pub mod global_state;
pub mod storage;

use crate::operations::GlobalOperation;
use crate::operations::handlers::GlobalOperationResponse;

use openraft::Entry;
use proven_topology::{Node, NodeId};
use serde::{Deserialize, Serialize};
use std::io::Cursor;

// Re-export main types
pub use global_state::{ConsensusGroupInfo, GlobalState, StreamData, StreamInfo};

// Import stream-related types from stream storage
pub use crate::config::stream::{RetentionPolicy, StreamConfig};

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub GlobalConsensusTypeConfig:
        D = GlobalOperation,
        R = GlobalOperationResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<GlobalConsensusTypeConfig>,
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
