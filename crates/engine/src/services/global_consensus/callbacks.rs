//! Global consensus callbacks implementation for the service

use std::sync::Arc;

use proven_topology::NodeId;

use crate::{
    consensus::global::GlobalConsensusCallbacks,
    error::ConsensusResult,
    foundation::events::EventBus,
    foundation::{
        GlobalStateRead, GlobalStateReader, GroupInfo, StreamName, models::stream::StreamPlacement,
        routing::RoutingTable, types::ConsensusGroupId,
    },
    services::global_consensus::events::{
        GlobalConsensusEvent, GlobalStateSnapshot, GroupSnapshot, StreamSnapshot,
    },
};

/// GlobalConsensusCallbacks implementation for GlobalConsensusService
pub struct GlobalConsensusCallbacksImpl {
    node_id: NodeId,
    event_bus: Option<Arc<EventBus>>,
    global_state: GlobalStateReader,
    routing_table: Arc<RoutingTable>,
}

impl GlobalConsensusCallbacksImpl {
    /// Create new callbacks implementation
    pub fn new(
        node_id: NodeId,
        event_bus: Option<Arc<EventBus>>,
        global_state: GlobalStateReader,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        Self {
            node_id,
            event_bus,
            global_state,
            routing_table,
        }
    }
}

#[async_trait::async_trait]
impl GlobalConsensusCallbacks for GlobalConsensusCallbacksImpl {
    async fn on_state_synchronized(&self) -> ConsensusResult<()> {
        tracing::info!(
            "Global state synchronized - updating routing table for node {}",
            self.node_id
        );

        // Get all groups and streams
        let all_groups = self.global_state.get_all_groups().await;
        let all_streams = self.global_state.get_all_streams().await;

        if let Some(ref event_bus) = self.event_bus {
            // Ensure all streams are initialized in their respective groups
            use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

            for stream in &all_streams {
                // Only ensure initialization for group streams
                if let StreamPlacement::Group(group_id) = stream.placement {
                    let ensure_cmd = EnsureStreamInitializedInGroup {
                        group_id,
                        stream_name: stream.stream_name.clone(),
                    };

                    // Use fire-and-forget since this is idempotent and we're in a callback
                    let event_bus_clone = event_bus.clone();
                    tokio::spawn(async move {
                        if let Err(e) = event_bus_clone.request(ensure_cmd).await {
                            tracing::debug!(
                                "Failed to ensure stream is initialized during state sync: {}",
                                e
                            );
                        }
                    });
                }
            }

            // Still publish event for other subscribers
            let snapshot = GlobalStateSnapshot {
                groups: all_groups
                    .into_iter()
                    .map(|g| GroupSnapshot {
                        group_id: g.id,
                        members: g.members,
                    })
                    .collect(),
                streams: all_streams
                    .into_iter()
                    .map(|s| StreamSnapshot {
                        stream_name: s.stream_name,
                        config: s.config,
                        placement: s.placement,
                    })
                    .collect(),
            };

            let event = GlobalConsensusEvent::StateSynchronized {
                snapshot: Box::new(snapshot),
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_group_created(
        &self,
        group_id: ConsensusGroupId,
        group_info: &GroupInfo,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "GlobalConsensusCallbacks: on_group_created called for group {:?} with {} members",
            group_id,
            group_info.members.len()
        );

        if let Some(ref event_bus) = self.event_bus {
            // Send command to ensure the group is initialized in group consensus service
            use crate::services::group_consensus::commands::EnsureGroupConsensusInitialized;
            let ensure_group_cmd = EnsureGroupConsensusInitialized {
                group_id,
                members: group_info.members.clone(),
            };

            tracing::info!(
                "GlobalConsensusCallbacks: Sending EnsureGroupConsensusInitialized command for group {:?}",
                group_id
            );

            // Use fire-and-forget since this is a callback
            let event_bus_clone = event_bus.clone();
            tokio::spawn(async move {
                if let Err(e) = event_bus_clone.request(ensure_group_cmd).await {
                    tracing::error!("Failed to ensure group initialization via command: {}", e);
                }
            });

            // Also emit event for other subscribers (like stream service)
            let event = GlobalConsensusEvent::GroupCreated {
                group_id,
                members: group_info.members.clone(),
            };
            event_bus.emit(event);
        } else {
            tracing::warn!(
                "GlobalConsensusCallbacks: No event bus available to update routing or send commands"
            );
        }

        Ok(())
    }

    async fn on_group_dissolved(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // TODO: Handle group dissolution
        tracing::info!("Group dissolved: {:?}", group_id);

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::GroupDissolved { group_id };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_stream_created(
        &self,
        stream_name: &StreamName,
        config: &crate::foundation::StreamConfig,
        placement: &StreamPlacement,
    ) -> ConsensusResult<()> {
        if let Some(ref event_bus) = self.event_bus {
            // Register the stream in the stream service
            // This works for both the originating node and other nodes learning about it
            use crate::services::stream::commands::RegisterStream;
            let register_cmd = RegisterStream {
                stream_name: stream_name.clone(),
                config: config.clone(),
                placement: *placement,
            };

            if let Err(e) = event_bus.request(register_cmd).await {
                tracing::error!(
                    "Failed to register stream {} with placement {:?}: {}",
                    stream_name,
                    placement,
                    e
                );
            }

            match placement {
                StreamPlacement::Group(group_id) => {
                    // Ensure the stream is initialized in the group consensus synchronously
                    use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

                    let ensure_cmd = EnsureStreamInitializedInGroup {
                        group_id: *group_id,
                        stream_name: stream_name.clone(),
                    };

                    // Ensure the stream is initialized in group consensus before continuing
                    if let Err(e) = event_bus.request(ensure_cmd).await {
                        tracing::error!(
                            "Failed to ensure stream {} is initialized in group {:?}: {}",
                            stream_name,
                            group_id,
                            e
                        );
                    }
                }
                StreamPlacement::Global => {
                    // Global streams don't need to be created in a specific group
                    // They are managed by global consensus itself
                    tracing::info!("Global stream {} created", stream_name);
                }
            }

            // Still emit event for other subscribers
            let event = GlobalConsensusEvent::StreamCreated {
                stream_name: stream_name.clone(),
                config: config.clone(),
                placement: *placement,
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_stream_deleted(&self, stream_name: &StreamName) -> ConsensusResult<()> {
        // Use command pattern to delete stream synchronously
        if let Some(ref event_bus) = self.event_bus {
            use crate::services::stream::commands::DeleteStream;

            // Delete the stream through command handler
            let delete_cmd = DeleteStream {
                name: stream_name.clone(),
            };

            // This will delete the stream synchronously before we continue
            if let Err(e) = event_bus.request(delete_cmd).await {
                tracing::error!("Failed to delete stream {}: {}", stream_name, e);
            }

            // Still emit event for other services
            let event = GlobalConsensusEvent::StreamDeleted {
                stream_name: stream_name.clone(),
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_membership_changed(
        &self,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Global consensus membership changed: added {:?}, removed {:?}",
            added_members,
            removed_members
        );

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::MembershipChanged {
                added_members: added_members.to_vec(),
                removed_members: removed_members.to_vec(),
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_leader_changed(
        &self,
        old_leader: Option<NodeId>,
        new_leader: Option<NodeId>,
        term: u64,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Global consensus leader changed from {:?} to {:?} (term {})",
            old_leader,
            new_leader,
            term
        );

        if let Some(ref event_bus) = self.event_bus {
            // Still publish event for other subscribers
            let event = GlobalConsensusEvent::LeaderChanged {
                old_leader,
                new_leader,
                term,
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_stream_reassigned(
        &self,
        stream_name: &StreamName,
        old_placement: &StreamPlacement,
        new_placement: &StreamPlacement,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Stream {} reassigned from {:?} to {:?}",
            stream_name,
            old_placement,
            new_placement
        );

        if let Some(ref event_bus) = self.event_bus {
            // Handle the reassignment based on placement types
            match (old_placement, new_placement) {
                (StreamPlacement::Group(_old_group), StreamPlacement::Group(new_group)) => {
                    // Moving between groups - ensure it's removed from old and added to new
                    use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

                    // Initialize in new group
                    let ensure_cmd = EnsureStreamInitializedInGroup {
                        group_id: *new_group,
                        stream_name: stream_name.clone(),
                    };

                    if let Err(e) = event_bus.request(ensure_cmd).await {
                        tracing::error!(
                            "Failed to ensure stream {} is initialized in new group {:?}: {}",
                            stream_name,
                            new_group,
                            e
                        );
                    }
                }
                (StreamPlacement::Group(_), StreamPlacement::Global) => {
                    // Moving from group to global - stream stays in group storage but routing changes
                    tracing::info!("Stream {} promoted to global placement", stream_name);
                }
                (StreamPlacement::Global, StreamPlacement::Group(group_id)) => {
                    // Moving from global to group - ensure it's initialized in the group
                    use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

                    let ensure_cmd = EnsureStreamInitializedInGroup {
                        group_id: *group_id,
                        stream_name: stream_name.clone(),
                    };

                    if let Err(e) = event_bus.request(ensure_cmd).await {
                        tracing::error!(
                            "Failed to ensure stream {} is initialized in group {:?}: {}",
                            stream_name,
                            group_id,
                            e
                        );
                    }
                }
                (StreamPlacement::Global, StreamPlacement::Global) => {
                    // No-op, shouldn't happen
                    tracing::warn!("Stream {} reassigned from global to global?", stream_name);
                }
            }
        }

        Ok(())
    }

    async fn on_global_stream_appended(
        &self,
        stream_name: &StreamName,
        entries: Arc<Vec<bytes::Bytes>>,
    ) -> ConsensusResult<()> {
        tracing::debug!(
            "Global stream {} appended with {} entries",
            stream_name,
            entries.len()
        );

        // Persist the messages for global streams
        if let Some(ref event_bus) = self.event_bus {
            use crate::services::stream::commands::PersistMessages;

            let command = PersistMessages {
                stream_name: stream_name.clone(),
                entries: entries.clone(),
            };

            if let Err(e) = event_bus.request(command).await {
                tracing::error!(
                    "Failed to persist messages for global stream {}: {}",
                    stream_name,
                    e
                );
            }
        }

        // Note: We could emit an event here if other services need to know about global stream appends

        Ok(())
    }
}
