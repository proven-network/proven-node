//! Group consensus operations

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::{ConsensusError, ConsensusResult, ErrorKind};
use crate::foundation::{traits::OperationHandler, types::OperationId};

use super::state::GroupState;
use super::types::{AdminOperation, GroupRequest, GroupResponse, StreamOperation};

/// Group consensus operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOperation {
    /// Operation ID
    pub id: OperationId,
    /// The request
    pub request: GroupRequest,
    /// Timestamp
    pub timestamp: u64,
}

impl GroupOperation {
    /// Create a new operation
    pub fn new(request: GroupRequest) -> Self {
        Self {
            id: OperationId::new(),
            request,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Handler for group consensus operations
pub struct GroupOperationHandler {
    /// Group state
    state: Arc<GroupState>,
}

impl GroupOperationHandler {
    /// Create a new handler
    pub fn new(state: Arc<GroupState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl OperationHandler for GroupOperationHandler {
    type Operation = GroupOperation;
    type Response = GroupResponse;

    async fn handle(&self, operation: Self::Operation) -> ConsensusResult<Self::Response> {
        match operation.request {
            GroupRequest::Stream(stream_op) => self.handle_stream_operation(stream_op).await,
            GroupRequest::Admin(admin_op) => self.handle_admin_operation(admin_op).await,
        }
    }

    async fn validate(&self, operation: &Self::Operation) -> ConsensusResult<()> {
        match &operation.request {
            GroupRequest::Stream(StreamOperation::Append { message, .. }) => {
                // Validate message size (example: 1MB limit)
                if message.payload.len() > 1024 * 1024 {
                    return Err(ConsensusError::with_context(
                        ErrorKind::Validation,
                        "Message exceeds size limit",
                    ));
                }

                Ok(())
            }
            GroupRequest::Stream(StreamOperation::Trim { .. }) => Ok(()),
            GroupRequest::Admin(_) => Ok(()),
        }
    }
}

impl GroupOperationHandler {
    /// Handle stream operations
    async fn handle_stream_operation(
        &self,
        operation: StreamOperation,
    ) -> ConsensusResult<GroupResponse> {
        match operation {
            StreamOperation::Append { stream, message } => {
                // Check if stream exists
                if self.state.get_stream(&stream).await.is_none() {
                    return Ok(GroupResponse::Error {
                        message: format!("Stream {stream} not found"),
                    });
                }

                // Append message
                if let Some(sequence) = self.state.append_message(&stream, message).await {
                    Ok(GroupResponse::Appended { stream, sequence })
                } else {
                    Ok(GroupResponse::Error {
                        message: format!("Failed to append to stream {stream}"),
                    })
                }
            }

            StreamOperation::Trim { stream, up_to_seq } => {
                if let Some(new_start_seq) = self.state.trim_stream(&stream, up_to_seq).await {
                    Ok(GroupResponse::Trimmed {
                        stream,
                        new_start_seq,
                    })
                } else {
                    Ok(GroupResponse::Error {
                        message: format!("Stream {stream} not found"),
                    })
                }
            }
        }
    }

    /// Handle administrative operations
    async fn handle_admin_operation(
        &self,
        operation: AdminOperation,
    ) -> ConsensusResult<GroupResponse> {
        match operation {
            AdminOperation::InitializeStream { stream } => {
                if self.state.initialize_stream(stream.clone()).await {
                    Ok(GroupResponse::Success)
                } else {
                    Ok(GroupResponse::Error {
                        message: format!("Stream {stream} already exists"),
                    })
                }
            }

            AdminOperation::RemoveStream { stream } => {
                if self.state.remove_stream(&stream).await {
                    Ok(GroupResponse::Success)
                } else {
                    Ok(GroupResponse::Error {
                        message: format!("Stream {stream} not found"),
                    })
                }
            }

            AdminOperation::Compact => {
                // Compaction would be implemented here
                // For now, just return success
                Ok(GroupResponse::Success)
            }
        }
    }
}
