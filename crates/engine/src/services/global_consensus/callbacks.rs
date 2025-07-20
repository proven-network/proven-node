//! Global consensus callbacks implementation for the service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::{
    consensus::global::{GlobalConsensusCallbacks, types::GroupInfo},
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::{
        event::{Event, EventPublisher},
        group_consensus::GroupConsensusService,
        stream::{StreamName, StreamService},
    },
};

/// GlobalConsensusCallbacks implementation for GlobalConsensusService
pub struct GlobalConsensusCallbacksImpl<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    node_id: NodeId,
    event_publisher: Option<EventPublisher>,
    routing_service: Option<Arc<crate::services::routing::RoutingService>>,
    group_consensus_service: Option<Arc<GroupConsensusService<T, G, S>>>,
    stream_service: Option<Arc<StreamService<S>>>,
}

impl<T, G, S> GlobalConsensusCallbacksImpl<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Create new callbacks implementation
    pub fn new(
        node_id: NodeId,
        event_publisher: Option<EventPublisher>,
        routing_service: Option<Arc<crate::services::routing::RoutingService>>,
        group_consensus_service: Option<Arc<GroupConsensusService<T, G, S>>>,
        stream_service: Option<Arc<StreamService<S>>>,
    ) -> Self {
        Self {
            node_id,
            event_publisher,
            routing_service,
            group_consensus_service,
            stream_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> GlobalConsensusCallbacks for GlobalConsensusCallbacksImpl<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn on_state_synchronized(
        &self,
        state: &crate::consensus::global::state::GlobalState,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Global state synchronized - performing initial sync for node {}",
            self.node_id
        );

        // Update routing table with all groups and stream assignments
        if let Some(ref routing) = self.routing_service {
            let all_groups = state.get_all_groups().await;
            let all_streams = state.get_all_streams().await;

            // Count streams per group for load balancing
            let mut stream_counts: std::collections::HashMap<ConsensusGroupId, usize> =
                std::collections::HashMap::new();
            for stream_info in &all_streams {
                *stream_counts.entry(stream_info.group_id).or_insert(0) += 1;
            }

            // Update routing table with all groups
            for group_info in &all_groups {
                let stream_count = stream_counts.get(&group_info.id).copied().unwrap_or(0);

                let group_route = crate::services::routing::GroupRoute {
                    group_id: group_info.id,
                    members: group_info.members.clone(),
                    leader: None,
                    stream_count,
                    health: crate::services::routing::GroupHealth::Healthy,
                    last_updated: std::time::SystemTime::now(),
                    location: if group_info.members.contains(&self.node_id) {
                        crate::services::routing::GroupLocation::Local
                    } else {
                        crate::services::routing::GroupLocation::Remote
                    },
                };

                if let Err(e) = routing.update_group_info(group_info.id, group_route).await {
                    tracing::error!(
                        "Failed to update routing for group {:?}: {}",
                        group_info.id,
                        e
                    );
                }
            }

            // Then update routing table with all stream assignments
            for stream_info in all_streams {
                if let Err(e) = routing
                    .update_stream_assignment(stream_info.name.to_string(), stream_info.group_id)
                    .await
                {
                    tracing::error!(
                        "Failed to update routing for stream {} -> group {:?}: {}",
                        stream_info.name,
                        stream_info.group_id,
                        e
                    );
                }
            }
        }

        // Create local group instances for groups where this node is a member
        let groups = state.get_all_groups().await;
        tracing::info!("Found {} total groups in global state", groups.len());

        let my_groups: Vec<_> = groups
            .into_iter()
            .filter(|g| g.members.contains(&self.node_id))
            .collect();

        tracing::info!(
            "Node {} is a member of {} groups",
            self.node_id,
            my_groups.len()
        );

        if let Some(ref group_consensus) = self.group_consensus_service {
            for group_info in my_groups.iter() {
                tracing::info!(
                    "Synchronizing local group {:?} with members: {:?}",
                    group_info.id,
                    group_info.members
                );
                if let Err(e) = group_consensus
                    .create_group(group_info.id, group_info.members.clone())
                    .await
                {
                    tracing::error!(
                        "Failed to synchronize local group {:?}: {}",
                        group_info.id,
                        e
                    );
                }
            }
        } else {
            tracing::warn!("No group consensus service available during state sync");
        }

        // Restore streams for groups where this node is a member
        if let Some(ref stream_service) = self.stream_service {
            let all_streams = state.get_all_streams().await;
            tracing::info!("Found {} total streams in global state", all_streams.len());

            // Get the group IDs where this node is a member
            let my_group_ids: std::collections::HashSet<_> =
                my_groups.iter().map(|g| g.id).collect();
            tracing::info!("This node is a member of groups: {:?}", my_group_ids);

            // Filter streams that belong to our groups
            for stream_info in all_streams {
                if my_group_ids.contains(&stream_info.group_id) {
                    tracing::info!(
                        "Restoring stream {} in group {:?}",
                        stream_info.name,
                        stream_info.group_id
                    );

                    // Create the stream in StreamService
                    if let Err(e) = stream_service
                        .create_stream(
                            stream_info.name.clone(),
                            stream_info.config.clone(),
                            stream_info.group_id,
                        )
                        .await
                    {
                        // Log at error level to see what's happening
                        tracing::error!(
                            "Failed to restore stream {} in group {:?}: {}",
                            stream_info.name,
                            stream_info.group_id,
                            e
                        );
                    } else {
                        tracing::info!(
                            "Successfully restored stream {} in group {:?}",
                            stream_info.name,
                            stream_info.group_id
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn on_group_created(
        &self,
        group_id: ConsensusGroupId,
        group_info: &GroupInfo,
    ) -> ConsensusResult<()> {
        // Update routing for the new group
        if let Some(ref routing) = self.routing_service {
            let group_route = crate::services::routing::GroupRoute {
                group_id,
                members: group_info.members.clone(),
                leader: None,
                stream_count: 0, // New groups start with 0 streams
                health: crate::services::routing::GroupHealth::Healthy,
                last_updated: std::time::SystemTime::now(),
                location: if group_info.members.contains(&self.node_id) {
                    crate::services::routing::GroupLocation::Local
                } else {
                    crate::services::routing::GroupLocation::Remote
                },
            };
            if let Err(e) = routing.update_group_info(group_id, group_route).await {
                tracing::error!("Failed to update routing for group {:?}: {}", group_id, e);
            }
        }

        // Check if this node is a member of the group
        if group_info.members.contains(&self.node_id) {
            // Direct service communication for immediate consistency
            if let Some(ref group_consensus) = self.group_consensus_service {
                tracing::info!(
                    "Creating local group {:?} via direct service communication",
                    group_id
                );
                if let Err(e) = group_consensus
                    .create_group(group_id, group_info.members.clone())
                    .await
                {
                    tracing::error!(
                        "Failed to create local group {:?} directly: {}",
                        group_id,
                        e
                    );
                }
            }
        }

        // Publish event for other services
        if let Some(ref publisher) = self.event_publisher {
            let event = Event::GroupCreated {
                group_id,
                members: group_info.members.clone(),
            };
            let source = format!("global-consensus-{}", self.node_id);
            if let Err(e) = publisher.publish(event, source).await {
                tracing::warn!("Failed to publish GroupCreated event: {}", e);
            }
        }

        Ok(())
    }

    async fn on_group_dissolved(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // TODO: Handle group dissolution
        tracing::info!("Group dissolved: {:?}", group_id);
        Ok(())
    }

    async fn on_stream_created(
        &self,
        stream_name: &StreamName,
        config: &crate::services::stream::StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Update routing service immediately
        if let Some(ref routing) = self.routing_service {
            // Update stream assignment
            if let Err(e) = routing
                .update_stream_assignment(stream_name.to_string(), group_id)
                .await
            {
                tracing::error!(
                    "Failed to update routing for stream {} -> group {:?}: {}",
                    stream_name,
                    group_id,
                    e
                );
            }

            // Increment stream count for the group
            if let Err(e) = routing.increment_stream_count(group_id).await {
                tracing::error!(
                    "Failed to increment stream count for group {:?}: {}",
                    group_id,
                    e
                );
            }
        }

        // Check if we're a member of the group that owns this stream
        if let Some(ref routing) = self.routing_service
            && let Ok(location_info) = routing.get_group_location(group_id).await
            && location_info.is_local
        {
            tracing::info!(
                "Initializing stream {} in local group {:?} via direct service communication",
                stream_name,
                group_id
            );

            // Initialize the stream in group consensus immediately
            if let Some(ref group_consensus) = self.group_consensus_service {
                let request = crate::consensus::group::GroupRequest::Admin(
                    crate::consensus::group::types::AdminOperation::InitializeStream {
                        stream: stream_name.clone(),
                    },
                );

                match group_consensus.submit_to_group(group_id, request).await {
                    Ok(_) => {
                        tracing::debug!(
                            "Successfully initialized stream {} in group {:?}",
                            stream_name,
                            group_id
                        );

                        // We successfully initialized in group consensus, now create in StreamService
                        if let Some(ref stream_service) = self.stream_service
                            && let Err(e) = stream_service
                                .create_stream(stream_name.clone(), config.clone(), group_id)
                                .await
                        {
                            // Stream might already exist, which is fine during replay
                            tracing::debug!(
                                "Failed to create stream {} in StreamService: {}",
                                stream_name,
                                e
                            );
                        }
                    }
                    Err(e) if e.is_not_leader() => {
                        // We're not the leader - this is expected and fine
                        // The leader will handle the initialization
                        tracing::debug!(
                            "Not the leader for group {:?}, stream {} will be initialized by leader",
                            group_id,
                            stream_name
                        );
                    }
                    Err(e) => {
                        // This is a real error
                        tracing::error!(
                            "Failed to initialize stream {} in group {:?}: {}",
                            stream_name,
                            group_id,
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn on_stream_deleted(&self, stream_name: &StreamName) -> ConsensusResult<()> {
        // Update routing service
        if let Some(ref routing) = self.routing_service {
            // First get the stream's group to decrement its count
            if let Ok(Some(stream_route)) = routing
                .get_stream_routing_info(&stream_name.to_string())
                .await
            {
                // Decrement stream count for the group
                if let Err(e) = routing.decrement_stream_count(stream_route.group_id).await {
                    tracing::error!(
                        "Failed to decrement stream count for group {:?}: {}",
                        stream_route.group_id,
                        e
                    );
                }
            }

            // Remove the stream assignment
            if let Err(e) = routing
                .remove_stream_assignment(&stream_name.to_string())
                .await
            {
                tracing::error!(
                    "Failed to remove routing for deleted stream {}: {}",
                    stream_name,
                    e
                );
            }
        }

        tracing::info!("Stream deleted: {}", stream_name);
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
        Ok(())
    }
}
