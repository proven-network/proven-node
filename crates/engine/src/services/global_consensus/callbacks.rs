//! Global consensus callbacks implementation for the service

use std::sync::Arc;

use proven_topology::NodeId;

use crate::{
    consensus::global::GlobalConsensusCallbacks,
    error::ConsensusResult,
    foundation::events::EventBus,
    foundation::{
        GlobalStateRead, GlobalStateReader, GroupInfo, StreamName, routing::RoutingTable,
        types::ConsensusGroupId,
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

        // Update routing table directly
        for group in &all_groups {
            if let Err(e) = self
                .routing_table
                .update_group_route(group.id, group.members.clone())
                .await
            {
                tracing::error!("Failed to update routing for group {:?}: {}", group.id, e);
            }
        }

        for stream in &all_streams {
            if let Err(e) = self
                .routing_table
                .update_stream_route(stream.name.to_string(), stream.group_id)
                .await
            {
                tracing::error!("Failed to update routing for stream {}: {}", stream.name, e);
            }
        }

        if let Some(ref event_bus) = self.event_bus {
            // Ensure all streams are initialized in their respective groups
            use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

            for stream in &all_streams {
                let ensure_cmd = EnsureStreamInitializedInGroup {
                    group_id: stream.group_id,
                    stream_name: stream.name.clone(),
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
                        stream_name: s.name,
                        config: s.config,
                        group_id: s.group_id,
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

        // Update routing table directly
        if let Err(e) = self
            .routing_table
            .update_group_route(group_id, group_info.members.clone())
            .await
        {
            tracing::error!("Failed to update routing for group {:?}: {}", group_id, e);
        }

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
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Update routing table directly
        if let Err(e) = self
            .routing_table
            .update_stream_route(stream_name.to_string(), group_id)
            .await
        {
            tracing::error!("Failed to update routing for stream {}: {}", stream_name, e);
        }

        if let Some(ref event_bus) = self.event_bus {
            // Create the stream through command handler
            use crate::services::stream::commands::CreateStream;
            let create_cmd = CreateStream {
                name: stream_name.clone(),
                config: config.clone(),
                group_id,
            };

            // This will create the stream synchronously before we continue
            if let Err(e) = event_bus.request(create_cmd).await {
                tracing::error!(
                    "Failed to create stream {} in group {:?}: {}",
                    stream_name,
                    group_id,
                    e
                );
            }

            // Ensure the stream is initialized in the group consensus synchronously
            use crate::services::group_consensus::commands::EnsureStreamInitializedInGroup;

            let ensure_cmd = EnsureStreamInitializedInGroup {
                group_id,
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

            // Still emit event for other subscribers
            let event = GlobalConsensusEvent::StreamCreated {
                stream_name: stream_name.clone(),
                config: config.clone(),
                group_id,
            };
            event_bus.emit(event);
        }

        Ok(())
    }

    async fn on_stream_deleted(&self, stream_name: &StreamName) -> ConsensusResult<()> {
        // Update routing table directly
        if let Err(e) = self
            .routing_table
            .remove_stream_route(&stream_name.to_string())
            .await
        {
            tracing::error!("Failed to remove routing for stream {}: {}", stream_name, e);
        }

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
}
