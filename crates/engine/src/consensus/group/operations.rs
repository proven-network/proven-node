//! Group consensus operations

use async_trait::async_trait;
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::{
    GroupStateRead, GroupStateWrite, GroupStateWriter, traits::OperationHandler, types::OperationId,
};

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
    /// Group ID
    group_id: crate::foundation::types::ConsensusGroupId,
    /// Group state
    state: GroupStateWriter,
}

impl GroupOperationHandler {
    /// Create a new handler
    pub fn new(
        group_id: crate::foundation::types::ConsensusGroupId,
        state: GroupStateWriter,
    ) -> Self {
        Self { group_id, state }
    }

    /// Get the group ID
    pub fn group_id(&self) -> crate::foundation::types::ConsensusGroupId {
        self.group_id
    }
}

#[async_trait]
impl OperationHandler for GroupOperationHandler {
    type Operation = GroupOperation;
    type Response = GroupResponse;

    async fn handle(
        &self,
        operation: Self::Operation,
        is_replay: bool,
    ) -> ConsensusResult<Self::Response> {
        // Only validate for current operations (skip for replay)
        if !is_replay {
            self.validate(&operation).await?;
        }
        match operation.request {
            GroupRequest::Stream(stream_op) => {
                self.handle_stream_operation(stream_op, is_replay).await
            }
            GroupRequest::Admin(admin_op) => self.handle_admin_operation(admin_op, is_replay).await,
        }
    }

    async fn validate(&self, operation: &Self::Operation) -> ConsensusResult<()> {
        match &operation.request {
            GroupRequest::Stream(StreamOperation::Append { messages, .. }) => {
                // Validate batch is not empty
                if messages.is_empty() {
                    return Err(Error::with_context(
                        ErrorKind::Validation,
                        "Cannot append empty batch",
                    ));
                }

                // Validate each message size (example: 1MB limit per message)
                for message in messages {
                    if message.payload.len() > 1024 * 1024 {
                        return Err(Error::with_context(
                            ErrorKind::Validation,
                            "Message exceeds size limit",
                        ));
                    }
                }

                // Could also validate total batch size here if needed

                Ok(())
            }
            GroupRequest::Stream(StreamOperation::Trim { .. }) => Ok(()),
            GroupRequest::Stream(StreamOperation::Delete { .. }) => {
                // LogIndex already guarantees the sequence is greater than zero
                Ok(())
            }
            GroupRequest::Admin(_) => Ok(()),
        }
    }
}

impl GroupOperationHandler {
    /// Handle stream operations
    async fn handle_stream_operation(
        &self,
        operation: StreamOperation,
        _is_replay: bool,
    ) -> ConsensusResult<GroupResponse> {
        match operation {
            StreamOperation::Append {
                stream_name,
                messages,
                timestamp,
            } => {
                // Check if stream exists and get its current state
                let initial_last_seq = match self.state.get_stream(&stream_name).await {
                    Some(state) => state.last_sequence,
                    _ => {
                        return Ok(GroupResponse::error(format!(
                            "Stream {stream_name} not found"
                        )));
                    }
                };

                let message_count = messages.len() as u64;

                // Append messages - returns pre-serialized entries
                let entries = self
                    .state
                    .append_to_group_stream(&stream_name, messages, timestamp)
                    .await;

                // Calculate the last sequence number after appending
                // If initial_last_seq was None (no messages), the first message is at sequence 1
                let last_seq = match initial_last_seq {
                    None => LogIndex::new(message_count).unwrap(),
                    Some(last) => last.saturating_add(message_count),
                };

                Ok(GroupResponse::Appended {
                    stream_name,
                    sequence: last_seq,
                    entries: Some(entries),
                })
            }

            StreamOperation::Trim {
                stream_name,
                up_to_seq,
            } => {
                if let Some(new_start_seq) = self.state.trim_stream(&stream_name, up_to_seq).await {
                    Ok(GroupResponse::Trimmed {
                        stream_name,
                        new_start_seq,
                    })
                } else {
                    Ok(GroupResponse::error(format!(
                        "Stream {stream_name} not found"
                    )))
                }
            }

            StreamOperation::Delete {
                stream_name,
                sequence,
            } => {
                // Check if stream exists
                if self.state.get_stream(&stream_name).await.is_none() {
                    return Ok(GroupResponse::error(format!(
                        "Stream {stream_name} not found"
                    )));
                }

                // Delete the message
                if let Some(deleted_seq) = self.state.delete_message(&stream_name, sequence).await {
                    Ok(GroupResponse::Deleted {
                        stream_name,
                        sequence: deleted_seq,
                    })
                } else {
                    Ok(GroupResponse::error(format!(
                        "Failed to delete message at sequence {sequence} from stream {stream_name}"
                    )))
                }
            }
        }
    }

    /// Handle administrative operations
    async fn handle_admin_operation(
        &self,
        operation: AdminOperation,
        _is_replay: bool,
    ) -> ConsensusResult<GroupResponse> {
        match operation {
            AdminOperation::InitializeStream { stream_name } => {
                if self.state.initialize_stream(stream_name.clone()).await {
                    Ok(GroupResponse::success())
                } else {
                    Ok(GroupResponse::error(format!(
                        "Stream {stream_name} already exists"
                    )))
                }
            }

            AdminOperation::RemoveStream { stream_name } => {
                if self.state.remove_stream(&stream_name).await {
                    Ok(GroupResponse::success())
                } else {
                    Ok(GroupResponse::error(format!(
                        "Stream {stream_name} not found"
                    )))
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
