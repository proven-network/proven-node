//! Type-safe request and response definitions
//!
//! This module provides traits and types for strongly-typed
//! request/response patterns in the consensus system.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

use crate::ConsensusGroupId;
use crate::error::Error as ConsensusError;
use crate::operations::{GlobalOperation, GroupStreamOperation};

/// Trait for type-safe requests with associated response types
pub trait Request: Send + Sync + Debug {
    /// The response type for this request
    type Response: Send + Sync + Debug;

    /// The error type for this request
    type Error: From<ConsensusError> + Send + Sync + Debug;

    /// Validate the request before processing
    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Base trait for consensus responses
pub trait ConsensusResponse: Send + Sync + Debug {
    /// Whether the operation succeeded
    fn is_success(&self) -> bool;

    /// Get error message if failed
    fn error(&self) -> Option<&str>;
}

/// Request wrapper for global consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRequest {
    /// Unique request identifier
    pub request_id: Uuid,
    /// The operation to perform
    pub operation: GlobalOperation,
    /// Optional request context
    pub context: Option<RequestContext>,
}

impl Request for GlobalRequest {
    type Response = GlobalResponse;
    type Error = ConsensusError;

    fn validate(&self) -> Result<(), Self::Error> {
        // Delegate to operation validation if available
        Ok(())
    }
}

/// Response from global consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalResponse {
    /// Request identifier this responds to
    pub request_id: Uuid,
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful
    pub sequence: Option<u64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Additional response data
    pub data: Option<ResponseData>,
}

impl ConsensusResponse for GlobalResponse {
    fn is_success(&self) -> bool {
        self.success
    }

    fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Request wrapper for local consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRequest {
    /// Unique request identifier
    pub request_id: Uuid,
    /// Target consensus group
    pub group_id: ConsensusGroupId,
    /// The operation to perform
    pub operation: GroupStreamOperation,
    /// Optional request context
    pub context: Option<RequestContext>,
}

impl Request for GroupRequest {
    type Response = GroupResponse;
    type Error = ConsensusError;

    fn validate(&self) -> Result<(), Self::Error> {
        // Validate group exists, operation is valid, etc.
        Ok(())
    }
}

/// Response from local consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResponse {
    /// Request identifier this responds to
    pub request_id: Uuid,
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

impl ConsensusResponse for GroupResponse {
    fn is_success(&self) -> bool {
        self.success
    }

    fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}

/// Unified request type that can be either global or local
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusRequest {
    /// Global consensus request
    Global(GlobalRequest),
    /// Local consensus request
    Local(GroupRequest),
}

impl ConsensusRequest {
    /// Get the request ID
    pub fn request_id(&self) -> Uuid {
        match self {
            ConsensusRequest::Global(req) => req.request_id,
            ConsensusRequest::Local(req) => req.request_id,
        }
    }

    /// Check if this is a global request
    pub fn is_global(&self) -> bool {
        matches!(self, ConsensusRequest::Global(_))
    }

    /// Check if this is a local request
    pub fn is_local(&self) -> bool {
        matches!(self, ConsensusRequest::Local(_))
    }
}

/// Optional context for requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestContext {
    /// Client ID making the request
    pub client_id: Option<String>,
    /// Request timeout in milliseconds
    pub timeout_ms: Option<u64>,
    /// Request priority
    pub priority: RequestPriority,
    /// Additional metadata
    pub metadata: Option<serde_json::Value>,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self {
            client_id: None,
            timeout_ms: None,
            priority: RequestPriority::Normal,
            metadata: None,
        }
    }
}

/// Request priority levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RequestPriority {
    /// Low priority request
    Low,
    /// Normal priority (default)
    Normal,
    /// High priority request
    High,
    /// Critical priority (system operations)
    Critical,
}

/// Additional response data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseData {
    /// Stream information
    StreamInfo {
        name: String,
        group_id: ConsensusGroupId,
        sequence: u64,
    },
    /// Group information
    GroupInfo {
        group_id: ConsensusGroupId,
        members: Vec<proven_topology::NodeId>,
        leader: Option<proven_topology::NodeId>,
    },
    /// Generic JSON data
    Json(serde_json::Value),
    /// Binary data
    Binary(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::{GlobalOperation, StreamManagementOperation};

    #[test]
    fn test_request_validation() {
        let request = GlobalRequest {
            request_id: Uuid::new_v4(),
            operation: GlobalOperation::StreamManagement(StreamManagementOperation::Create {
                name: "test-stream".to_string(),
                config: Default::default(),
                group_id: ConsensusGroupId::new(1),
            }),
            context: None,
        };

        assert!(request.validate().is_ok());
    }

    #[test]
    fn test_response_success() {
        let response = GlobalResponse {
            request_id: Uuid::new_v4(),
            success: true,
            sequence: Some(42),
            error: None,
            data: None,
        };

        assert!(response.is_success());
        assert!(response.error().is_none());
    }
}
