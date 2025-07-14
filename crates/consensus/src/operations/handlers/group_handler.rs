//! Handler for consensus group operations

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::{
    ConsensusGroupId, NodeId,
    core::{
        engine::events::{ConsensusEvent, EventBus, EventResult, create_reply_channel},
        global::{ConsensusGroupInfo, GlobalState},
    },
    error::ConsensusResult,
    operations::GroupOperation,
};

use super::{GlobalOperationHandler, GroupOperationResponse, OperationContext};

/// Handler for consensus group operations
pub struct GroupOperationHandler {
    /// Event bus for cross-layer communication
    event_bus: Option<Arc<EventBus>>,
}

impl GroupOperationHandler {
    /// Create a new group operation handler
    pub fn new(event_bus: Option<Arc<EventBus>>) -> Self {
        Self { event_bus }
    }

    /// Handle group creation
    async fn handle_create_group(
        &self,
        group_id: ConsensusGroupId,
        initial_members: &[NodeId],
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GroupOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Check if group already exists
        if groups.contains_key(&group_id) {
            return Ok(GroupOperationResponse::Failed {
                operation: "create_group".to_string(),
                reason: format!("Consensus group {group_id:?} already exists"),
            });
        }

        // Validate members list
        if initial_members.is_empty() {
            return Ok(GroupOperationResponse::Failed {
                operation: "create_group".to_string(),
                reason: "Cannot create group with no members".to_string(),
            });
        }

        // Create group info
        let group_info = ConsensusGroupInfo {
            id: group_id,
            members: initial_members.to_vec(),
            stream_count: 0,
            created_at: context.timestamp,
        };

        groups.insert(group_id, group_info);

        info!(
            "Created consensus group {:?} with {} members",
            group_id,
            initial_members.len()
        );

        // Emit event for cross-layer coordination if event bus is available
        if let Some(event_bus) = &self.event_bus {
            let (reply_to, rx) = create_reply_channel();
            let event = ConsensusEvent::GroupCreated {
                group_id,
                members: initial_members.to_vec(),
                reply_to,
            };

            let _ = event_bus.publish(event).await;

            // Wait for local layer to initialize the group (with timeout)
            match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
                Ok(Ok(result)) => {
                    match result {
                        EventResult::Failed(reason) => {
                            // Local initialization failed, rollback
                            groups.remove(&group_id);
                            return Ok(GroupOperationResponse::Failed {
                                operation: "create_group".to_string(),
                                reason: format!("Failed to initialize group locally: {reason}"),
                            });
                        }
                        EventResult::Success => {
                            debug!("Group initialized successfully in local layer");
                        }
                        EventResult::Pending(_) => {
                            debug!("Group initialization is pending in local layer");
                        }
                    }
                }
                Ok(Err(_)) => {
                    debug!("Local layer did not respond to group creation");
                }
                Err(_) => {
                    debug!("Timeout waiting for local layer to initialize group");
                }
            }
        }

        Ok(GroupOperationResponse::Created {
            sequence: context.sequence,
            group_id,
            members: initial_members.to_vec(),
        })
    }

    /// Handle group deletion
    async fn handle_delete_group(
        &self,
        group_id: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GroupOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Check if group exists
        let group_info = match groups.get(&group_id) {
            Some(info) => info.clone(),
            None => {
                return Ok(GroupOperationResponse::Failed {
                    operation: "delete_group".to_string(),
                    reason: format!("Consensus group {group_id:?} not found"),
                });
            }
        };

        // Check if group has active streams
        if group_info.stream_count > 0 {
            return Ok(GroupOperationResponse::Failed {
                operation: "delete_group".to_string(),
                reason: format!(
                    "Cannot delete group {:?} with {} active streams",
                    group_id, group_info.stream_count
                ),
            });
        }

        // Remove the group
        groups.remove(&group_id);

        info!("Deleted consensus group {:?}", group_id);

        // Emit event for cross-layer coordination if event bus is available
        if let Some(event_bus) = &self.event_bus {
            let (reply_to, _rx) = create_reply_channel();
            let event = ConsensusEvent::GroupDeleted { group_id, reply_to };
            let _ = event_bus.publish(event).await;
        }

        Ok(GroupOperationResponse::Deleted {
            sequence: context.sequence,
            group_id,
        })
    }

    /// Handle updating group members
    async fn handle_update_members(
        &self,
        group_id: ConsensusGroupId,
        new_members: &[NodeId],
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GroupOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Check if group exists
        let group_info = match groups.get_mut(&group_id) {
            Some(info) => info,
            None => {
                return Ok(GroupOperationResponse::Failed {
                    operation: "update_members".to_string(),
                    reason: format!("Consensus group {group_id:?} not found"),
                });
            }
        };

        // Validate new members list
        if new_members.is_empty() {
            return Ok(GroupOperationResponse::Failed {
                operation: "update_members".to_string(),
                reason: "Cannot update group to have no members".to_string(),
            });
        }

        let old_members = group_info.members.clone();
        group_info.members = new_members.to_vec();

        info!(
            "Updated consensus group {:?} members from {:?} to {:?}",
            group_id, old_members, new_members
        );

        // Calculate added/removed members
        let added: Vec<NodeId> = new_members
            .iter()
            .filter(|m| !old_members.contains(m))
            .cloned()
            .collect();
        let removed: Vec<NodeId> = old_members
            .iter()
            .filter(|m| !new_members.contains(m))
            .cloned()
            .collect();

        Ok(GroupOperationResponse::MembersUpdated {
            sequence: context.sequence,
            group_id,
            added,
            removed,
        })
    }
}

#[async_trait]
impl GlobalOperationHandler for GroupOperationHandler {
    type Operation = GroupOperation;
    type Response = GroupOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GroupOperationResponse> {
        match operation {
            GroupOperation::Create {
                group_id,
                initial_members,
            } => {
                self.handle_create_group(*group_id, initial_members, state, context)
                    .await
            }
            GroupOperation::Delete { group_id } => {
                self.handle_delete_group(*group_id, state, context).await
            }
            GroupOperation::UpdateMembers { group_id, members } => {
                self.handle_update_members(*group_id, members, state, context)
                    .await
            }
        }
    }

    async fn post_execute(
        &self,
        operation: &Self::Operation,
        response: &Self::Response,
        _state: &GlobalState,
        _context: &OperationContext,
    ) -> ConsensusResult<()> {
        match response {
            GroupOperationResponse::Failed {
                operation: op,
                reason,
            } => {
                debug!("Failed to execute group operation {}: {}", op, reason);
            }
            _ => {
                debug!("Successfully executed group operation: {:?}", operation);
            }
        }
        Ok(())
    }

    fn operation_type(&self) -> &'static str {
        "GroupOperation"
    }
}
