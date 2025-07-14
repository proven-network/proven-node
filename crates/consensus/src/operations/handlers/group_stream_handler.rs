//! Handler infrastructure for group stream operations
//!
//! This module provides traits and implementations for handling operations
//! within consensus groups (stream data, migration, pubsub, maintenance).

use async_trait::async_trait;

use crate::{
    ConsensusGroupId, NodeId,
    core::state_machine::LocalStateMachine,
    error::{ConsensusResult, Error},
    operations::{
        GroupStreamOperation, MaintenanceOperation, MigrationOperation, PubSubOperation,
        StreamOperation,
    },
};

use super::group_responses::{
    GroupStreamOperationResponse, MaintenanceOperationResponse, MigrationOperationResponse,
    PubSubOperationResponse, StreamOperationResponse,
};

/// Context for executing group operations
#[derive(Debug, Clone)]
pub struct GroupOperationContext {
    /// The sequence number of this operation in the log
    pub sequence: u64,
    /// The timestamp when this operation was created
    pub timestamp: u64,
    /// The node that proposed this operation
    pub proposer_node_id: NodeId,
    /// The consensus group this operation is for
    pub group_id: ConsensusGroupId,
    /// Whether this node is currently the leader of the group
    pub is_leader: bool,
}

impl GroupOperationContext {
    /// Create a new group operation context
    pub fn new(
        sequence: u64,
        proposer_node_id: NodeId,
        group_id: ConsensusGroupId,
        is_leader: bool,
    ) -> Self {
        Self {
            sequence,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            proposer_node_id,
            group_id,
            is_leader,
        }
    }
}

/// Trait for handling group stream operations
#[async_trait]
pub trait GroupStreamOperationHandler: Send + Sync {
    /// The specific operation type this handler processes
    type Operation: Send + Sync;
    /// The specific response type this handler returns
    type Response: Into<GroupStreamOperationResponse> + Send + Sync;

    /// Apply the operation to the local state
    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &mut LocalStateMachine,
        context: &GroupOperationContext,
    ) -> ConsensusResult<Self::Response>;

    /// Pre-execution hook for setup or additional validation
    async fn pre_execute(
        &self,
        _operation: &Self::Operation,
        _state: &LocalStateMachine,
        _context: &GroupOperationContext,
    ) -> ConsensusResult<()> {
        Ok(())
    }

    /// Post-execution hook for cleanup, metrics, or event emission
    async fn post_execute(
        &self,
        _operation: &Self::Operation,
        _response: &Self::Response,
        _state: &LocalStateMachine,
        _context: &GroupOperationContext,
    ) -> ConsensusResult<()> {
        Ok(())
    }

    /// Get the operation type name for logging and metrics
    fn operation_type(&self) -> &'static str;
}

/// Registry for group stream operation handlers
pub struct GroupStreamHandlerRegistry {
    stream_handler: Box<
        dyn GroupStreamOperationHandler<
                Operation = StreamOperation,
                Response = StreamOperationResponse,
            >,
    >,
    migration_handler: Box<
        dyn GroupStreamOperationHandler<
                Operation = MigrationOperation,
                Response = MigrationOperationResponse,
            >,
    >,
    pubsub_handler: Box<
        dyn GroupStreamOperationHandler<
                Operation = PubSubOperation,
                Response = PubSubOperationResponse,
            >,
    >,
    maintenance_handler: Box<
        dyn GroupStreamOperationHandler<
                Operation = MaintenanceOperation,
                Response = MaintenanceOperationResponse,
            >,
    >,
}

impl GroupStreamHandlerRegistry {
    /// Create a new registry with default handlers
    pub fn with_defaults() -> Self {
        Self {
            stream_handler: Box::new(StreamDataHandler),
            migration_handler: Box::new(MigrationHandler),
            pubsub_handler: Box::new(PubSubHandler),
            maintenance_handler: Box::new(MaintenanceHandler),
        }
    }

    /// Handle a group stream operation
    pub async fn handle_operation(
        &self,
        operation: &GroupStreamOperation,
        state: &mut LocalStateMachine,
        context: &GroupOperationContext,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        match operation {
            GroupStreamOperation::Stream(op) => {
                let response = self.stream_handler.apply(op, state, context).await?;
                Ok(response.into())
            }
            GroupStreamOperation::Migration(op) => {
                let response = self.migration_handler.apply(op, state, context).await?;
                Ok(response.into())
            }
            GroupStreamOperation::PubSub(op) => {
                let response = self.pubsub_handler.apply(op, state, context).await?;
                Ok(response.into())
            }
            GroupStreamOperation::Maintenance(op) => {
                let response = self.maintenance_handler.apply(op, state, context).await?;
                Ok(response.into())
            }
        }
    }
}

/// Handler for stream data operations
struct StreamDataHandler;

#[async_trait]
impl GroupStreamOperationHandler for StreamDataHandler {
    type Operation = StreamOperation;
    type Response = StreamOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &mut LocalStateMachine,
        context: &GroupOperationContext,
    ) -> ConsensusResult<Self::Response> {
        use crate::operations::StreamOperation::*;

        match operation {
            Publish {
                stream,
                data,
                metadata,
            } => {
                // Apply the publish operation to state
                let sequence = state
                    .publish_message(
                        stream,
                        data.clone(),
                        metadata
                            .as_ref()
                            .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                    )
                    .await
                    .map_err(Error::storage)?;

                Ok(StreamOperationResponse::Written {
                    sequence,
                    stream_name: stream.clone(),
                    data_size: data.len(),
                })
            }
            PublishBatch { stream, messages } => {
                // Apply batch publish - publish each message individually
                let mut last_sequence = 0;
                let mut total_size = 0;

                for data in messages {
                    last_sequence = state
                        .publish_message(stream, data.clone(), None)
                        .await
                        .map_err(Error::storage)?;
                    total_size += data.len();
                }

                Ok(StreamOperationResponse::Written {
                    sequence: last_sequence,
                    stream_name: stream.clone(),
                    data_size: total_size,
                })
            }
            Rollup {
                stream,
                data,
                expected_seq: _,
            } => {
                // Rollup is not directly supported in LocalStateMachine
                // For now, just publish it as a regular message
                let sequence = state
                    .publish_message(
                        stream,
                        data.clone(),
                        Some(
                            [("type".to_string(), "rollup".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                    )
                    .await
                    .map_err(Error::storage)?;

                Ok(StreamOperationResponse::Written {
                    sequence,
                    stream_name: stream.clone(),
                    data_size: data.len(),
                })
            }
            Delete { stream, sequence } => {
                // Apply delete operation
                state
                    .delete_message(stream, *sequence)
                    .await
                    .map_err(Error::storage)?;

                Ok(StreamOperationResponse::Truncated {
                    sequence: context.sequence,
                    stream_name: stream.clone(),
                    messages_removed: 1,
                })
            }
            DirectGet { stream, sequence } => {
                // Read from the stream
                match state
                    .get_message(stream, *sequence)
                    .await
                    .map_err(Error::storage)?
                {
                    Some(data) => Ok(StreamOperationResponse::Read {
                        sequence: *sequence,
                        stream_name: stream.clone(),
                        data,
                        metadata: None,
                    }),
                    None => Ok(StreamOperationResponse::Failed {
                        operation: "DirectGet".to_string(),
                        reason: format!("Message at sequence {sequence} not found"),
                    }),
                }
            }
        }
    }

    fn operation_type(&self) -> &'static str {
        "stream_data"
    }
}

/// Handler for migration operations
struct MigrationHandler;

#[async_trait]
impl GroupStreamOperationHandler for MigrationHandler {
    type Operation = MigrationOperation;
    type Response = MigrationOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &mut LocalStateMachine,
        context: &GroupOperationContext,
    ) -> ConsensusResult<Self::Response> {
        use crate::operations::MigrationOperation::*;

        match operation {
            CreateStream {
                stream_name,
                source_group,
            } => {
                // Create stream for migration
                let config = crate::config::stream::StreamConfig::default();
                state
                    .create_stream(stream_name, config)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::Initiated {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    source_group: *source_group,
                    target_group: context.group_id,
                })
            }
            GetCheckpoint { stream_name } => {
                // Get checkpoint metadata
                let checkpoint = state
                    .create_checkpoint_metadata(stream_name)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::SnapshotTransferred {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    messages_count: checkpoint.message_count as usize,
                    bytes_transferred: checkpoint.total_bytes as usize,
                })
            }
            GetIncrementalCheckpoint {
                stream_name,
                since_sequence: _,
            } => {
                // Get incremental checkpoint - use regular checkpoint for now
                let checkpoint = state
                    .create_checkpoint_metadata(stream_name)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::SnapshotTransferred {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    messages_count: checkpoint.message_count as usize,
                    bytes_transferred: checkpoint.total_bytes as usize,
                })
            }
            ApplyCheckpoint { checkpoint: _ } => {
                // In a real implementation, we would deserialize the checkpoint
                // For now, just return a placeholder response
                Ok(MigrationOperationResponse::Completed {
                    sequence: context.sequence,
                    stream_name: "migrated_stream".to_string(), // Would extract from checkpoint
                    total_messages: 0,                          // Would extract from checkpoint
                    duration_ms: 0, // Would calculate in real implementation
                })
            }
            ApplyIncrementalCheckpoint { checkpoint } => {
                // In a real implementation, we would deserialize the checkpoint
                // For now, just return a placeholder response
                Ok(MigrationOperationResponse::SnapshotTransferred {
                    sequence: context.sequence,
                    stream_name: "migrated_stream".to_string(), // Would extract from checkpoint
                    messages_count: 0,                          // Would extract from checkpoint
                    bytes_transferred: checkpoint.len(),
                })
            }
            PauseStream { stream_name } => {
                // Pause stream
                state
                    .pause_stream(stream_name)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::Initiated {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    source_group: context.group_id,
                    target_group: context.group_id, // Same group, just pausing
                })
            }
            ResumeStream { stream_name } => {
                // Resume stream
                let resumed_sequences = state
                    .resume_stream(stream_name)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::Completed {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    total_messages: resumed_sequences.len(),
                    duration_ms: 0,
                })
            }
            RemoveStream { stream_name } => {
                // Remove stream
                state
                    .remove_stream(stream_name)
                    .await
                    .map_err(Error::storage)?;

                Ok(MigrationOperationResponse::Completed {
                    sequence: context.sequence,
                    stream_name: stream_name.clone(),
                    total_messages: 0,
                    duration_ms: 0,
                })
            }
        }
    }

    fn operation_type(&self) -> &'static str {
        "migration"
    }
}

/// Handler for PubSub operations
struct PubSubHandler;

#[async_trait]
impl GroupStreamOperationHandler for PubSubHandler {
    type Operation = PubSubOperation;
    type Response = PubSubOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &mut LocalStateMachine,
        _context: &GroupOperationContext,
    ) -> ConsensusResult<Self::Response> {
        use crate::operations::PubSubOperation::*;

        match operation {
            PublishFromPubSub {
                stream_name,
                subject,
                data,
                source: _,
            } => {
                // Apply PubSub publish as regular message with metadata
                let mut headers = std::collections::HashMap::new();
                headers.insert("pubsub_subject".to_string(), subject.clone());

                let sequence = state
                    .publish_message(stream_name, data.clone(), Some(headers))
                    .await
                    .map_err(Error::storage)?;

                Ok(PubSubOperationResponse::Published {
                    sequence,
                    stream_name: stream_name.clone(),
                    subject: subject.clone(),
                    subscribers_notified: 0, // Would get from state
                })
            }
        }
    }

    fn operation_type(&self) -> &'static str {
        "pubsub"
    }
}

/// Handler for maintenance operations
struct MaintenanceHandler;

#[async_trait]
impl GroupStreamOperationHandler for MaintenanceHandler {
    type Operation = MaintenanceOperation;
    type Response = MaintenanceOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        _state: &mut LocalStateMachine,
        context: &GroupOperationContext,
    ) -> ConsensusResult<Self::Response> {
        use crate::operations::MaintenanceOperation::*;

        match operation {
            GetMetrics => {
                // Get metrics - for now return success
                Ok(MaintenanceOperationResponse::Verified {
                    sequence: context.sequence,
                    entries_checked: 0,
                    errors_found: 0,
                    errors: vec![],
                })
            }
            CleanupPendingOperations { max_age_secs: _ } => {
                // Cleanup old operations
                Ok(MaintenanceOperationResponse::Compacted {
                    sequence: context.sequence,
                    entries_before: 0,
                    entries_after: 0,
                    bytes_reclaimed: 0,
                })
            }
        }
    }

    fn operation_type(&self) -> &'static str {
        "maintenance"
    }
}
