//! Operations for the consensus system
//!
//! This module contains the operations for the consensus system, including
//! global admin operations and local stream operations.

use crate::allocation::ConsensusGroupId;
use crate::global::GlobalOperation;
use crate::local::LocalStreamOperation;

use serde::{Deserialize, Serialize};

/// Unified operation type that can be routed to appropriate consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusOperation {
    /// Global admin operation
    GlobalAdmin(GlobalOperation),
    /// Local stream operation
    LocalStream {
        /// Target consensus group
        group_id: ConsensusGroupId,
        /// The operation
        operation: LocalStreamOperation,
    },
}

/// Request for consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusRequest {
    /// The operation to perform
    pub operation: ConsensusOperation,
    /// Optional request ID for tracking
    pub request_id: Option<String>,
}

/// Response from consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful (for stream operations)
    pub sequence: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Request ID if provided
    pub request_id: Option<String>,
    /// Checkpoint data (for checkpoint operations)
    pub checkpoint_data: Option<bytes::Bytes>,
}
