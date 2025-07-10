//! State command pattern for explicit state machine operations
//!
//! This module implements the command pattern for state machine operations,
//! ensuring that validation, application, and persistence are handled consistently.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::global_state::GlobalState;
use super::{GlobalResponse, StreamConfig};
use crate::NodeId;
use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::operations::GlobalOperation;
use crate::operations::{GroupOperation, RoutingOperation, StreamManagementOperation};

/// Trait for state machine commands that can be validated, applied, and persisted
#[async_trait]
pub trait StateCommand: Send + Sync + Debug {
    /// Validate the command against the current state
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()>;

    /// Apply the command to the state, returning a response
    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse;

    /// Convert the command to bytes for persistence
    fn persist(&self) -> Vec<u8>;

    /// Get the command type for metrics and logging
    fn command_type(&self) -> &'static str;
}

/// Command processor that handles the complete lifecycle of a command
pub struct CommandProcessor;

impl CommandProcessor {
    /// Process a command through validation, application, and persistence
    pub async fn process<C: StateCommand + ?Sized>(
        command: &C,
        state: &GlobalState,
        sequence: u64,
    ) -> ConsensusResult<GlobalResponse> {
        // First validate the command
        command.validate(state).await?;

        // Apply the command to the state
        let response = command.apply(state, sequence).await;

        // If successful, the persistence will be handled by the storage layer
        // which will receive the serialized command

        Ok(response)
    }
}

// Stream Commands

/// Command to create a new stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateStreamCommand {
    /// The name of the stream to create
    pub stream_name: String,
    /// The configuration for the stream
    pub config: StreamConfig,
    /// The ID of the consensus group to create the stream in initially
    pub group_id: ConsensusGroupId,
}

#[async_trait]
impl StateCommand for CreateStreamCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        // Stream name validation should be handled by StreamManagementOperationValidator
        // Here we only check state-specific constraints

        // Check if stream already exists
        let configs = state.stream_configs.read().await;
        if configs.contains_key(&self.stream_name) {
            return Err(Error::Stream(crate::error::StreamError::AlreadyExists {
                name: self.stream_name.clone(),
            }));
        }
        drop(configs);

        // Check if consensus group exists
        let groups = state.consensus_groups.read().await;
        if !groups.contains_key(&self.group_id) {
            return Err(Error::Group(crate::error::GroupError::NotFound {
                id: self.group_id,
            }));
        }

        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::StreamManagement(
                    StreamManagementOperation::Create {
                        name: self.stream_name.clone(),
                        config: self.config.clone(),
                        group_id: self.group_id,
                    },
                ),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "CreateStream"
    }
}

/// Command to delete a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStreamCommand {
    /// The name of the stream to delete
    pub stream_name: String,
}

#[async_trait]
impl StateCommand for DeleteStreamCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        let configs = state.stream_configs.read().await;
        if !configs.contains_key(&self.stream_name) {
            return Err(Error::Stream(crate::error::StreamError::NotFound {
                name: self.stream_name.clone(),
            }));
        }
        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::StreamManagement(
                    StreamManagementOperation::Delete {
                        name: self.stream_name.clone(),
                    },
                ),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "DeleteStream"
    }
}

/// Command to reallocate a stream to a different group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReallocateStreamCommand {
    /// The name of the stream to reallocate
    pub stream_name: String,
    /// The ID of the new consensus group to reallocate the stream to
    pub new_group: ConsensusGroupId,
}

#[async_trait]
impl StateCommand for ReallocateStreamCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        // Check stream exists
        let configs = state.stream_configs.read().await;
        if !configs.contains_key(&self.stream_name) {
            return Err(Error::Stream(crate::error::StreamError::NotFound {
                name: self.stream_name.clone(),
            }));
        }
        drop(configs);

        // Check new group exists
        let groups = state.consensus_groups.read().await;
        if !groups.contains_key(&self.new_group) {
            return Err(Error::Group(crate::error::GroupError::NotFound {
                id: self.new_group,
            }));
        }

        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::StreamManagement(
                    StreamManagementOperation::Reallocate {
                        name: self.stream_name.clone(),
                        target_group: self.new_group,
                    },
                ),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "ReallocateStream"
    }
}

// Group Commands

/// Command to add a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddConsensusGroupCommand {
    /// The ID of the consensus group to add
    pub group_id: ConsensusGroupId,
    /// The members to add to the consensus group
    pub members: Vec<NodeId>,
}

#[async_trait]
impl StateCommand for AddConsensusGroupCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        let groups = state.consensus_groups.read().await;
        if groups.contains_key(&self.group_id) {
            return Err(Error::Group(crate::error::GroupError::AlreadyExists {
                id: self.group_id,
            }));
        }

        if self.members.is_empty() {
            return Err(Error::Group(crate::error::GroupError::NoMembers {
                id: self.group_id,
            }));
        }

        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::Group(GroupOperation::Create {
                    group_id: self.group_id,
                    initial_members: self.members.clone(),
                }),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "AddConsensusGroup"
    }
}

/// Command to remove a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveConsensusGroupCommand {
    /// The ID of the consensus group to remove
    pub group_id: ConsensusGroupId,
}

#[async_trait]
impl StateCommand for RemoveConsensusGroupCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        let groups = state.consensus_groups.read().await;
        if !groups.contains_key(&self.group_id) {
            return Err(Error::Group(crate::error::GroupError::NotFound {
                id: self.group_id,
            }));
        }
        drop(groups);

        // Check no streams are allocated to this group
        let configs = state.stream_configs.read().await;
        let allocated_streams: Vec<_> = configs
            .iter()
            .filter(|(_, config)| config.consensus_group == Some(self.group_id))
            .map(|(name, _)| name.clone())
            .collect();

        if !allocated_streams.is_empty() {
            return Err(Error::Group(crate::error::GroupError::HasActiveStreams {
                id: self.group_id,
                count: allocated_streams.len(),
            }));
        }

        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::Group(GroupOperation::Delete {
                    group_id: self.group_id,
                }),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "RemoveConsensusGroup"
    }
}

// PubSub Commands

/// Command to subscribe a stream to a subject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeToSubjectCommand {
    /// The name of the stream to subscribe to
    pub stream_name: String,
    /// The subject pattern to subscribe to
    pub subject_pattern: String,
}

#[async_trait]
impl StateCommand for SubscribeToSubjectCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        // Validate subject pattern
        if let Err(e) = crate::pubsub::validate_subject_pattern(&self.subject_pattern) {
            return Err(Error::Stream(
                crate::error::StreamError::InvalidSubjectPattern {
                    pattern: self.subject_pattern.clone(),
                    reason: e.to_string(),
                },
            ));
        }

        // Stream doesn't need to exist - it will be created on demand
        let _ = state;
        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::Routing(RoutingOperation::Subscribe {
                    stream_name: self.stream_name.clone(),
                    subject_pattern: self.subject_pattern.clone(),
                }),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "SubscribeToSubject"
    }
}

/// Command to unsubscribe a stream from a subject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeFromSubjectCommand {
    /// The name of the stream to unsubscribe from
    pub stream_name: String,
    /// The subject pattern to unsubscribe from
    pub subject_pattern: String,
}

#[async_trait]
impl StateCommand for UnsubscribeFromSubjectCommand {
    async fn validate(&self, state: &GlobalState) -> ConsensusResult<()> {
        // Check if stream exists and has this subscription
        let streams = state.streams.read().await;
        if let Some(stream_data) = streams.get(&self.stream_name) {
            if !stream_data.subscriptions.contains(&self.subject_pattern) {
                return Err(Error::Stream(
                    crate::error::StreamError::OperationConflict {
                        name: self.stream_name.clone(),
                        reason: format!("Not subscribed to '{}'", self.subject_pattern),
                    },
                ));
            }
        }
        Ok(())
    }

    async fn apply(&self, state: &GlobalState, sequence: u64) -> GlobalResponse {
        state
            .apply_operation(
                &crate::operations::GlobalOperation::Routing(RoutingOperation::Unsubscribe {
                    stream_name: self.stream_name.clone(),
                    subject_pattern: self.subject_pattern.clone(),
                }),
                sequence,
            )
            .await
    }

    fn persist(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn command_type(&self) -> &'static str {
        "UnsubscribeFromSubject"
    }
}

/// Command factory to create commands from GlobalOperation
pub struct CommandFactory;

impl CommandFactory {
    /// Create a command from a GlobalOperation
    pub fn from_operation(operation: &GlobalOperation) -> Box<dyn StateCommand> {
        match operation {
            GlobalOperation::StreamManagement(StreamManagementOperation::Create {
                name,
                config,
                group_id,
            }) => Box::new(CreateStreamCommand {
                stream_name: name.clone(),
                config: config.clone(),
                group_id: *group_id,
            }),
            GlobalOperation::StreamManagement(StreamManagementOperation::Delete { name }) => {
                Box::new(DeleteStreamCommand {
                    stream_name: name.clone(),
                })
            }
            GlobalOperation::StreamManagement(StreamManagementOperation::Reallocate {
                name,
                target_group,
            }) => Box::new(ReallocateStreamCommand {
                stream_name: name.clone(),
                new_group: *target_group,
            }),
            GlobalOperation::Group(GroupOperation::Create {
                group_id,
                initial_members,
            }) => Box::new(AddConsensusGroupCommand {
                group_id: *group_id,
                members: initial_members.clone(),
            }),
            GlobalOperation::Group(GroupOperation::Delete { group_id }) => {
                Box::new(RemoveConsensusGroupCommand {
                    group_id: *group_id,
                })
            }
            GlobalOperation::Routing(RoutingOperation::Subscribe {
                stream_name,
                subject_pattern,
            }) => Box::new(SubscribeToSubjectCommand {
                stream_name: stream_name.clone(),
                subject_pattern: subject_pattern.clone(),
            }),
            GlobalOperation::Routing(RoutingOperation::Unsubscribe {
                stream_name,
                subject_pattern,
            }) => Box::new(UnsubscribeFromSubjectCommand {
                stream_name: stream_name.clone(),
                subject_pattern: subject_pattern.clone(),
            }),
            _ => panic!("Invalid operation: {:?}", operation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_stream_command_validation() {
        let state = GlobalState::new();

        // Add a consensus group first
        let add_group = AddConsensusGroupCommand {
            group_id: ConsensusGroupId::new(1),
            members: vec![NodeId::from_seed(1)],
        };

        assert!(add_group.validate(&state).await.is_ok());
        let response = add_group.apply(&state, 1).await;
        assert!(response.success);

        // Now create stream command should validate
        let create_stream = CreateStreamCommand {
            stream_name: "test-stream".to_string(),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(1),
        };

        assert!(create_stream.validate(&state).await.is_ok());

        // Create with non-existent group should fail validation
        let bad_create = CreateStreamCommand {
            stream_name: "test-stream2".to_string(),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(999),
        };

        assert!(bad_create.validate(&state).await.is_err());
    }

    #[tokio::test]
    async fn test_command_processor() {
        let state = GlobalState::new();

        // Add a group
        let add_group = AddConsensusGroupCommand {
            group_id: ConsensusGroupId::new(1),
            members: vec![NodeId::from_seed(1)],
        };

        let response = CommandProcessor::process(&add_group, &state, 1)
            .await
            .unwrap();
        assert!(response.success);

        // Try to add same group again - should fail validation
        let result = CommandProcessor::process(&add_group, &state, 2).await;
        assert!(result.is_err());
    }
}
