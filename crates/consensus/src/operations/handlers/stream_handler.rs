//! Handler for stream management operations

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::{
    ConsensusGroupId,
    core::{
        engine::events::{ConsensusEvent, EventBus, EventResult, create_reply_channel},
        global::{GlobalState, StreamConfig, StreamData},
        group::MigrationState,
    },
    error::ConsensusResult,
    operations::StreamManagementOperation,
};

use super::{GlobalOperationHandler, OperationContext, StreamManagementResponse};

/// Handler for stream management operations
pub struct StreamManagementHandler {
    /// Event bus for cross-layer communication
    event_bus: Option<Arc<EventBus>>,
}

impl StreamManagementHandler {
    /// Create a new stream management handler
    pub fn new(event_bus: Option<Arc<EventBus>>) -> Self {
        Self { event_bus }
    }

    /// Handle stream creation
    async fn handle_create_stream(
        &self,
        name: &str,
        config: &StreamConfig,
        group_id: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;
        let mut streams = state.streams.write().await;
        let mut groups = state.consensus_groups.write().await;

        // Check if stream already exists
        if configs.contains_key(name) {
            return Ok(StreamManagementResponse::Failed {
                operation: "create_stream".to_string(),
                reason: format!("Stream '{name}' already exists"),
            });
        }

        // Check if consensus group exists
        if !groups.contains_key(&group_id) {
            return Ok(StreamManagementResponse::Failed {
                operation: "create_stream".to_string(),
                reason: format!("Consensus group {group_id:?} not found"),
            });
        }

        // Create stream configuration with the assigned group
        let mut stream_config = config.clone();
        stream_config.consensus_group = Some(group_id);
        configs.insert(name.to_string(), stream_config.clone());

        // Initialize stream data
        streams
            .entry(name.to_string())
            .or_insert_with(|| StreamData {
                messages: std::collections::BTreeMap::new(),
                next_sequence: 1,
                subscriptions: std::collections::HashSet::new(),
            });

        // Update group stream count
        if let Some(group) = groups.get_mut(&group_id) {
            group.stream_count += 1;
        }

        info!("Created stream '{}' in group {:?}", name, group_id);

        // Emit event for cross-layer coordination if event bus is available
        if let Some(event_bus) = &self.event_bus {
            let (reply_to, rx) = create_reply_channel();
            let event = ConsensusEvent::StreamCreated {
                name: name.to_string(),
                config: stream_config,
                group_id,
                reply_to,
            };

            let _ = event_bus.publish(event).await;

            // Wait for local layer to initialize the stream (with timeout)
            match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
                Ok(Ok(result)) => {
                    match result {
                        EventResult::Failed(reason) => {
                            // Local initialization failed, rollback
                            configs.remove(name);
                            streams.remove(name);
                            if let Some(group) = groups.get_mut(&group_id) {
                                group.stream_count = group.stream_count.saturating_sub(1);
                            }
                            return Ok(StreamManagementResponse::Failed {
                                operation: "create_stream".to_string(),
                                reason: format!("Failed to initialize stream locally: {reason}"),
                            });
                        }
                        EventResult::Success => {
                            debug!("Stream initialized successfully in local layer");
                        }
                        EventResult::Pending(_) => {
                            debug!("Stream initialization is pending in local layer");
                        }
                    }
                }
                Ok(Err(_)) => {
                    debug!("Local layer did not respond to stream creation");
                }
                Err(_) => {
                    debug!("Timeout waiting for local layer to initialize stream");
                }
            }
        }

        Ok(StreamManagementResponse::Created {
            sequence: context.sequence,
            stream_name: name.to_string(),
            group_id,
        })
    }

    /// Handle stream deletion
    async fn handle_delete_stream(
        &self,
        name: &str,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;
        let mut streams = state.streams.write().await;
        let mut groups = state.consensus_groups.write().await;

        // Check if stream exists
        let stream_config = match configs.get(name) {
            Some(config) => config.clone(),
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "delete_stream".to_string(),
                    reason: format!("Stream '{name}' not found"),
                });
            }
        };

        // Get message count before deletion
        let message_count = streams
            .get(name)
            .map(|s| s.messages.len() as u64)
            .unwrap_or(0);

        // Remove stream configuration
        configs.remove(name);

        // Remove stream data
        streams.remove(name);

        // Update group stream count if applicable
        if let Some(group_id) = stream_config.consensus_group
            && let Some(group) = groups.get_mut(&group_id)
        {
            group.stream_count = group.stream_count.saturating_sub(1);
        }

        // Remove from subject router
        drop(configs);
        drop(streams);
        drop(groups);

        let mut router = state.subject_router.write().await;
        router.remove_stream(name);

        info!("Deleted stream '{}'", name);

        // Emit event for cross-layer coordination if event bus is available
        if let Some(event_bus) = &self.event_bus
            && let Some(group_id) = stream_config.consensus_group
        {
            let (reply_to, _rx) = create_reply_channel();
            let event = ConsensusEvent::StreamDeleted {
                name: name.to_string(),
                group_id,
                reply_to,
            };
            let _ = event_bus.publish(event).await;
        }

        Ok(StreamManagementResponse::Deleted {
            sequence: context.sequence,
            stream_name: name.to_string(),
            message_count,
        })
    }

    /// Handle stream reallocation
    async fn handle_reallocate_stream(
        &self,
        name: &str,
        target_group: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;
        let mut groups = state.consensus_groups.write().await;

        // Check if stream exists
        let current_config = match configs.get_mut(name) {
            Some(config) => config,
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "reallocate_stream".to_string(),
                    reason: format!("Stream '{name}' not found"),
                });
            }
        };

        // Check if target group exists
        if !groups.contains_key(&target_group) {
            return Ok(StreamManagementResponse::Failed {
                operation: "reallocate_stream".to_string(),
                reason: format!("Target consensus group {target_group:?} not found"),
            });
        }

        // Get current group
        let current_group = current_config.consensus_group;

        // Update stream counts
        if let Some(old_group_id) = current_group
            && let Some(old_group) = groups.get_mut(&old_group_id)
        {
            old_group.stream_count = old_group.stream_count.saturating_sub(1);
        }

        if let Some(new_group) = groups.get_mut(&target_group) {
            new_group.stream_count += 1;
        }

        // Update stream configuration
        current_config.consensus_group = Some(target_group);

        info!(
            "Reallocated stream '{}' from {:?} to {:?}",
            name, current_group, target_group
        );

        // TODO: Emit migration event for cross-layer coordination

        Ok(StreamManagementResponse::Reallocated {
            sequence: context.sequence,
            stream_name: name.to_string(),
            from_group: current_group.unwrap_or(ConsensusGroupId::new(0)),
            to_group: target_group,
        })
    }

    /// Handle stream configuration update
    async fn handle_update_config(
        &self,
        name: &str,
        new_config: &StreamConfig,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;

        // Check if stream exists
        let current_config = match configs.get_mut(name) {
            Some(config) => config,
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "update_stream_config".to_string(),
                    reason: format!("Stream '{name}' not found"),
                });
            }
        };

        // Track what configuration changed
        let mut changes = Vec::new();

        if current_config.retention_policy != new_config.retention_policy {
            changes.push("retention_policy".to_string());
        }
        if current_config.max_messages != new_config.max_messages {
            changes.push("max_messages".to_string());
        }
        if current_config.max_bytes != new_config.max_bytes {
            changes.push("max_bytes".to_string());
        }
        if current_config.max_age_secs != new_config.max_age_secs {
            changes.push("max_age_secs".to_string());
        }
        if current_config.storage_type != new_config.storage_type {
            changes.push("storage_type".to_string());
        }
        if current_config.pubsub_bridge_enabled != new_config.pubsub_bridge_enabled {
            changes.push("pubsub_bridge_enabled".to_string());
        }
        if current_config.compression != new_config.compression {
            changes.push("compression".to_string());
        }

        // Preserve the consensus group assignment
        let consensus_group = current_config.consensus_group;

        // Update configuration
        *current_config = new_config.clone();
        current_config.consensus_group = consensus_group;

        info!("Updated configuration for stream '{}': {:?}", name, changes);

        // Emit event for cross-layer coordination if event bus is available
        if let Some(event_bus) = &self.event_bus
            && let Some(group_id) = consensus_group
        {
            let (reply_to, _rx) = create_reply_channel();
            let event = ConsensusEvent::StreamConfigUpdated {
                name: name.to_string(),
                config: current_config.clone(),
                group_id,
                reply_to,
            };
            let _ = event_bus.publish(event).await;
        }

        Ok(StreamManagementResponse::ConfigUpdated {
            sequence: context.sequence,
            stream_name: name.to_string(),
            changes,
        })
    }

    /// Handle stream migration between groups
    async fn handle_migrate_stream(
        &self,
        name: &str,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
        migration_state: &MigrationState,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;
        let mut groups = state.consensus_groups.write().await;

        // Check if stream exists
        let stream_config = match configs.get_mut(name) {
            Some(config) => config,
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "migrate_stream".to_string(),
                    reason: format!("Stream '{name}' not found"),
                });
            }
        };

        // Verify current group matches from_group
        match stream_config.consensus_group {
            Some(current_group) if current_group == from_group => {}
            Some(current_group) => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "migrate_stream".to_string(),
                    reason: format!(
                        "Stream '{name}' is in group {current_group:?}, not {from_group:?}"
                    ),
                });
            }
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "migrate_stream".to_string(),
                    reason: format!("Stream '{name}' is not assigned to any group"),
                });
            }
        }

        // Check if target group exists
        if !groups.contains_key(&to_group) {
            return Ok(StreamManagementResponse::Failed {
                operation: "migrate_stream".to_string(),
                reason: format!("Target group {to_group:?} not found"),
            });
        }

        // Generate migration ID
        let migration_id = format!("{name}-{}-{}", from_group.0, to_group.0);

        info!(
            "Starting migration of stream '{}' from {:?} to {:?} (state: {:?})",
            name, from_group, to_group, migration_state
        );

        // Handle different migration states
        match migration_state {
            MigrationState::Preparing => {
                // Just log the preparing migration
                debug!("Migration {} is preparing", migration_id);
            }
            MigrationState::Transferring => {
                // Initial data transfer in progress
                debug!("Migration {} is transferring data", migration_id);
            }
            MigrationState::Syncing => {
                // Syncing incremental updates
                debug!("Migration {} is syncing updates", migration_id);
            }
            MigrationState::Switching => {
                // Switching traffic to target group
                debug!("Migration {} is switching traffic", migration_id);
            }
            MigrationState::Completing => {
                // Migration is being finalized
                debug!("Migration {} is completing", migration_id);
            }
            MigrationState::Completed => {
                // Migration completed, finalize the move
                // Update stream counts
                if let Some(old_group) = groups.get_mut(&from_group) {
                    old_group.stream_count = old_group.stream_count.saturating_sub(1);
                }
                if let Some(new_group) = groups.get_mut(&to_group) {
                    new_group.stream_count += 1;
                }

                // Update stream configuration
                stream_config.consensus_group = Some(to_group);

                info!("Migration {} completed successfully", migration_id);
            }
            MigrationState::Failed => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "migrate_stream".to_string(),
                    reason: "Migration failed".to_string(),
                });
            }
        }

        // Emit migration event for cross-layer coordination
        if let Some(event_bus) = &self.event_bus {
            let (reply_to, _rx) = create_reply_channel();
            let event = ConsensusEvent::StreamMigrating {
                name: name.to_string(),
                from_group,
                to_group,
                state: migration_state.clone(),
                reply_to,
            };
            let _ = event_bus.publish(event).await;
        }

        Ok(StreamManagementResponse::MigrationStarted {
            sequence: context.sequence,
            stream_name: name.to_string(),
            migration_id,
        })
    }

    /// Handle stream allocation update (simplified version of reallocation)
    async fn handle_update_allocation(
        &self,
        name: &str,
        new_group: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        let mut configs = state.stream_configs.write().await;
        let mut groups = state.consensus_groups.write().await;

        // Check if stream exists
        let stream_config = match configs.get_mut(name) {
            Some(config) => config,
            None => {
                return Ok(StreamManagementResponse::Failed {
                    operation: "update_stream_allocation".to_string(),
                    reason: format!("Stream '{name}' not found"),
                });
            }
        };

        // Check if new group exists
        if !groups.contains_key(&new_group) {
            return Ok(StreamManagementResponse::Failed {
                operation: "update_stream_allocation".to_string(),
                reason: format!("Target group {new_group:?} not found"),
            });
        }

        // Get current group
        let old_group = stream_config.consensus_group;

        // Update stream counts
        if let Some(old_group_id) = old_group
            && let Some(old_group) = groups.get_mut(&old_group_id)
        {
            old_group.stream_count = old_group.stream_count.saturating_sub(1);
        }

        if let Some(new_group_data) = groups.get_mut(&new_group) {
            new_group_data.stream_count += 1;
        }

        // Update stream configuration
        stream_config.consensus_group = Some(new_group);

        info!(
            "Updated allocation for stream '{}' from {:?} to {:?}",
            name, old_group, new_group
        );

        // Emit event for cross-layer coordination
        if let Some(event_bus) = &self.event_bus {
            let (reply_to, _rx) = create_reply_channel();
            let event = ConsensusEvent::StreamReallocated {
                name: name.to_string(),
                old_group,
                new_group,
                reply_to,
            };
            let _ = event_bus.publish(event).await;
        }

        Ok(StreamManagementResponse::Reallocated {
            sequence: context.sequence,
            stream_name: name.to_string(),
            from_group: old_group.unwrap_or(ConsensusGroupId::new(0)),
            to_group: new_group,
        })
    }
}

#[async_trait]
impl GlobalOperationHandler for StreamManagementHandler {
    type Operation = StreamManagementOperation;
    type Response = StreamManagementResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<StreamManagementResponse> {
        match operation {
            StreamManagementOperation::Create {
                name,
                config,
                group_id,
            } => {
                self.handle_create_stream(name, config, *group_id, state, context)
                    .await
            }
            StreamManagementOperation::Delete { name } => {
                self.handle_delete_stream(name, state, context).await
            }
            StreamManagementOperation::Reallocate { name, target_group } => {
                self.handle_reallocate_stream(name, *target_group, state, context)
                    .await
            }
            StreamManagementOperation::UpdateConfig { name, config } => {
                self.handle_update_config(name, config, state, context)
                    .await
            }
            StreamManagementOperation::Migrate {
                name,
                from_group,
                to_group,
                state: migration_state,
            } => {
                self.handle_migrate_stream(
                    name,
                    *from_group,
                    *to_group,
                    migration_state,
                    state,
                    context,
                )
                .await
            }
            StreamManagementOperation::UpdateAllocation { name, new_group } => {
                self.handle_update_allocation(name, *new_group, state, context)
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
            StreamManagementResponse::Failed {
                operation: op,
                reason,
            } => {
                debug!("Failed to execute stream operation {}: {}", op, reason);
            }
            _ => {
                debug!("Successfully executed stream operation: {:?}", operation);
            }
        }
        Ok(())
    }

    fn operation_type(&self) -> &'static str {
        "StreamManagement"
    }
}
