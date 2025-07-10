//! State command pattern for local stream operations
//!
//! This module implements the command pattern for local state machine operations,
//! ensuring that validation, application, and persistence are handled consistently.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

use super::state_machine::StorageBackedLocalState;
use super::{LocalRequest, LocalResponse};
use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::global::PubSubMessageSource;
use crate::operations::{
    LocalStreamOperation, MigrationOperation, PubSubOperation, StreamOperation,
};

/// Trait for local state machine commands that can be validated, applied, and persisted
#[async_trait]
pub trait LocalStateCommand: Send + Sync + Debug {
    /// Validate the command against the current state
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()>;

    /// Apply the command to the state, returning a response
    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse;

    /// Convert the command to a LocalRequest for persistence
    fn to_request(&self) -> LocalRequest;

    /// Get the command type for metrics and logging
    fn command_type(&self) -> &'static str;
}

/// Command processor that handles the complete lifecycle of a local command
pub struct LocalCommandProcessor;

impl LocalCommandProcessor {
    /// Process a command through validation, application, and persistence
    pub async fn process<C: LocalStateCommand + ?Sized>(
        command: &C,
        state: &mut StorageBackedLocalState,
    ) -> ConsensusResult<LocalResponse> {
        // First validate the command
        command.validate(state).await?;

        // Apply the command to the state
        let response = command.apply(state).await;

        // If successful, the persistence will be handled by the Raft layer
        // which will receive the LocalRequest

        Ok(response)
    }
}

// Stream Commands

/// Command to publish a message to a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishCommand {
    /// Stream name to publish to
    pub stream: String,
    /// Message data
    pub data: Bytes,
    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,
}

#[async_trait]
impl LocalStateCommand for PublishCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        // Additional validation could check stream limits, etc.
        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        match state
            .publish_message(&self.stream, self.data.clone(), self.metadata.clone())
            .await
        {
            Ok(sequence) => LocalResponse {
                success: true,
                sequence: Some(sequence),
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::publish_to_stream(
                self.stream.clone(),
                self.data.clone(),
                self.metadata.clone(),
            ),
        }
    }

    fn command_type(&self) -> &'static str {
        "Publish"
    }
}

/// Command to publish multiple messages to a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishBatchCommand {
    /// Stream name to publish to
    pub stream: String,
    /// Messages to publish
    pub messages: Vec<Bytes>,
}

#[async_trait]
impl LocalStateCommand for PublishBatchCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        // Check batch size
        if self.messages.is_empty() {
            return Err(Error::InvalidOperation("Batch cannot be empty".to_string()));
        }

        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        // Publish messages one by one since there's no batch method
        let mut last_sequence = 0u64;

        for message in &self.messages {
            match state
                .publish_message(&self.stream, message.clone(), None)
                .await
            {
                Ok(sequence) => last_sequence = sequence,
                Err(e) => {
                    return LocalResponse {
                        success: false,
                        sequence: None,
                        error: Some(format!("Batch publish failed: {}", e)),
                        checkpoint_data: None,
                    };
                }
            }
        }

        LocalResponse {
            success: true,
            sequence: Some(last_sequence),
            error: None,
            checkpoint_data: None,
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::publish_batch_to_stream(
                self.stream.clone(),
                self.messages.clone(),
            ),
        }
    }

    fn command_type(&self) -> &'static str {
        "PublishBatch"
    }
}

/// Command to delete a message from a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteMessageCommand {
    /// Stream name to delete from
    pub stream: String,
    /// Sequence number of the message to delete
    pub sequence: u64,
}

#[async_trait]
impl LocalStateCommand for DeleteMessageCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        // Note: Sequence validation happens in the state machine
        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        match state.delete_message(&self.stream, self.sequence).await {
            Ok(()) => LocalResponse {
                success: true,
                sequence: None,
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::delete_from_stream(self.stream.clone(), self.sequence),
        }
    }

    fn command_type(&self) -> &'static str {
        "DeleteMessage"
    }
}

// Migration Commands

/// Command to create a stream for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateStreamForMigrationCommand {
    /// Stream name to create
    pub stream_name: String,
    /// Source consensus group
    pub source_group: ConsensusGroupId,
}

#[async_trait]
impl LocalStateCommand for CreateStreamForMigrationCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream already exists
        match state.get_stream_config(&self.stream_name).await {
            Ok(_) => Err(Error::Stream(crate::error::StreamError::AlreadyExists {
                name: self.stream_name.clone(),
            })),
            Err(_) => Ok(()), // Stream doesn't exist, good to create
        }
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        // Create stream with default config
        // In real implementation, might want to get config from global state
        let config = crate::global::StreamConfig::default();

        match state.create_stream(&self.stream_name, config).await {
            Ok(()) => LocalResponse {
                success: true,
                sequence: None,
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::create_stream_for_migration(
                self.stream_name.clone(),
                self.source_group,
            ),
        }
    }

    fn command_type(&self) -> &'static str {
        "CreateStreamForMigration"
    }
}

/// Command to pause a stream for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseStreamCommand {
    /// Stream name to pause
    pub stream_name: String,
}

#[async_trait]
impl LocalStateCommand for PauseStreamCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream_name)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        match state.pause_stream(&self.stream_name).await {
            Ok(()) => LocalResponse {
                success: true,
                sequence: None,
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::pause_stream(self.stream_name.clone()),
        }
    }

    fn command_type(&self) -> &'static str {
        "PauseStream"
    }
}

/// Command to resume a stream after migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeStreamCommand {
    /// Stream name to resume
    pub stream_name: String,
}

#[async_trait]
impl LocalStateCommand for ResumeStreamCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream_name)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        match state.resume_stream(&self.stream_name).await {
            Ok(pending_sequences) => LocalResponse {
                success: true,
                sequence: if pending_sequences.is_empty() {
                    None
                } else {
                    Some(pending_sequences[0]) // Return first pending sequence
                },
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::resume_stream(self.stream_name.clone()),
        }
    }

    fn command_type(&self) -> &'static str {
        "ResumeStream"
    }
}

/// Command to remove a stream after migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveStreamCommand {
    /// Stream name to remove
    pub stream_name: String,
}

#[async_trait]
impl LocalStateCommand for RemoveStreamCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream_name)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        match state.remove_stream(&self.stream_name).await {
            Ok(()) => LocalResponse {
                success: true,
                sequence: None,
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::remove_stream(self.stream_name.clone()),
        }
    }

    fn command_type(&self) -> &'static str {
        "RemoveStream"
    }
}

// PubSub Commands

/// Command to publish from PubSub to a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishFromPubSubCommand {
    /// Stream name to publish to
    pub stream_name: String,
    /// Subject that triggered this
    pub subject: String,
    /// Message data
    pub data: Bytes,
    /// Source information
    pub source: PubSubMessageSource,
}

#[async_trait]
impl LocalStateCommand for PublishFromPubSubCommand {
    async fn validate(&self, state: &StorageBackedLocalState) -> ConsensusResult<()> {
        // Check if stream exists
        let config = state
            .get_stream_config(&self.stream_name)
            .await
            .map_err(Error::InvalidOperation)?;

        // Stream exists if we got config without error
        let _ = config;

        Ok(())
    }

    async fn apply(&self, state: &mut StorageBackedLocalState) -> LocalResponse {
        // Publish from PubSub is essentially a regular publish with metadata
        let mut metadata = HashMap::new();
        metadata.insert("subject".to_string(), self.subject.clone());
        metadata.insert(
            "source_node".to_string(),
            format!("{:?}", self.source.node_id),
        );

        match state
            .publish_message(&self.stream_name, self.data.clone(), Some(metadata))
            .await
        {
            Ok(sequence) => LocalResponse {
                success: true,
                sequence: Some(sequence),
                error: None,
                checkpoint_data: None,
            },
            Err(e) => LocalResponse {
                success: false,
                sequence: None,
                error: Some(e),
                checkpoint_data: None,
            },
        }
    }

    fn to_request(&self) -> LocalRequest {
        LocalRequest {
            operation: LocalStreamOperation::publish_from_pubsub(
                self.stream_name.clone(),
                self.subject.clone(),
                self.data.clone(),
                self.source.clone(),
            ),
        }
    }

    fn command_type(&self) -> &'static str {
        "PublishFromPubSub"
    }
}

/// Command factory to create commands from LocalStreamOperation
pub struct LocalCommandFactory;

impl LocalCommandFactory {
    /// Create a command from a LocalStreamOperation
    pub fn from_operation(operation: &LocalStreamOperation) -> Box<dyn LocalStateCommand> {
        match operation {
            LocalStreamOperation::Stream(StreamOperation::Publish {
                stream,
                data,
                metadata,
            }) => Box::new(PublishCommand {
                stream: stream.clone(),
                data: data.clone(),
                metadata: metadata.clone(),
            }),
            LocalStreamOperation::Stream(StreamOperation::PublishBatch { stream, messages }) => {
                Box::new(PublishBatchCommand {
                    stream: stream.clone(),
                    messages: messages.clone(),
                })
            }
            LocalStreamOperation::Stream(StreamOperation::Delete { stream, sequence }) => {
                Box::new(DeleteMessageCommand {
                    stream: stream.clone(),
                    sequence: *sequence,
                })
            }
            LocalStreamOperation::Migration(MigrationOperation::CreateStream {
                stream_name,
                source_group,
            }) => Box::new(CreateStreamForMigrationCommand {
                stream_name: stream_name.clone(),
                source_group: *source_group,
            }),
            LocalStreamOperation::Migration(MigrationOperation::PauseStream { stream_name }) => {
                Box::new(PauseStreamCommand {
                    stream_name: stream_name.clone(),
                })
            }
            LocalStreamOperation::Migration(MigrationOperation::ResumeStream { stream_name }) => {
                Box::new(ResumeStreamCommand {
                    stream_name: stream_name.clone(),
                })
            }
            LocalStreamOperation::Migration(MigrationOperation::RemoveStream { stream_name }) => {
                Box::new(RemoveStreamCommand {
                    stream_name: stream_name.clone(),
                })
            }
            LocalStreamOperation::PubSub(PubSubOperation::PublishFromPubSub {
                stream_name,
                subject,
                data,
                source,
            }) => Box::new(PublishFromPubSubCommand {
                stream_name: stream_name.clone(),
                subject: subject.clone(),
                data: data.clone(),
                source: source.clone(),
            }),
            // For operations that don't map to commands, panic or return a no-op command
            _ => panic!("No command implementation for operation: {:?}", operation),
        }
    }
}
