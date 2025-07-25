//! Global consensus callbacks implementation for the service

use std::sync::Arc;

use proven_topology::NodeId;

use crate::{
    consensus::global::GlobalConsensusCallbacks,
    error::ConsensusResult,
    foundation::events::EventBus,
    foundation::{
        GlobalState, GlobalStateRead, GlobalStateReader, GroupInfo, routing::RoutingTable,
        types::ConsensusGroupId,
    },
    services::{
        global_consensus::events::{
            GlobalConsensusEvent, GlobalStateSnapshot, GroupSnapshot, StreamSnapshot,
        },
        stream::StreamName,
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
                .update_group_route(
                    group.id,
                    group.members.clone(),
                    None, // Leader not known yet
                )
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
            .update_group_route(
                group_id,
                group_info.members.clone(),
                None, // Leader not known at creation time
            )
            .await
        {
            tracing::error!("Failed to update routing for group {:?}: {}", group_id, e);
        }

        if let Some(ref event_bus) = self.event_bus {
            // Still publish event for other subscribers
            let event = GlobalConsensusEvent::GroupCreated {
                group_id,
                members: group_info.members.clone(),
            };
            tracing::info!(
                "GlobalConsensusCallbacks: Emitting GroupCreated event for group {:?}",
                group_id
            );
            event_bus.emit(event);
        } else {
            tracing::warn!(
                "GlobalConsensusCallbacks: No event bus available to update routing or emit event"
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
        config: &crate::services::stream::StreamConfig,
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

        // Update routing table directly
        self.routing_table
            .update_global_leader(new_leader.clone())
            .await;

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
