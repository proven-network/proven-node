//! Global consensus event subscriber for routing service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::foundation::types::ConsensusGroupId;
use crate::services::global_consensus::events::GlobalConsensusEvent;
use crate::services::routing::{GroupLocation, GroupRoute, RoutingTable};
use proven_topology::NodeId;

/// Subscriber for global consensus events that updates routing information
#[derive(Clone)]
pub struct GlobalConsensusSubscriber {
    routing_table: Arc<RoutingTable>,
    local_node_id: NodeId,
}

impl GlobalConsensusSubscriber {
    /// Create a new global consensus subscriber
    pub fn new(routing_table: Arc<RoutingTable>, local_node_id: NodeId) -> Self {
        Self {
            routing_table,
            local_node_id,
        }
    }
}

#[async_trait]
impl EventHandler<GlobalConsensusEvent> for GlobalConsensusSubscriber {
    async fn handle(&self, event: GlobalConsensusEvent, _metadata: EventMetadata) {
        match event {
            GlobalConsensusEvent::StateSynchronized { snapshot } => {
                // This is a critical synchronous event
                info!(
                    "GlobalConsensusSubscriber: Handling StateSynchronized event with {} groups and {} streams",
                    snapshot.groups.len(),
                    snapshot.streams.len()
                );

                // Update routing table with all groups from snapshot
                for group in &snapshot.groups {
                    let is_member = group.members.contains(&self.local_node_id);
                    let location = if is_member {
                        GroupLocation::Local
                    } else {
                        GroupLocation::Remote
                    };

                    // Count streams for this group
                    let stream_count = snapshot
                        .streams
                        .iter()
                        .filter(|s| s.group_id == group.group_id)
                        .count();

                    let route = GroupRoute {
                        group_id: group.group_id,
                        location,
                        members: group.members.clone(),
                        leader: None, // Leader will be set by group consensus events
                        stream_count,
                        health: crate::services::routing::types::GroupHealth::Healthy,
                        last_updated: std::time::SystemTime::now(),
                    };

                    if let Err(e) = self
                        .routing_table
                        .update_group_route(group.group_id, route)
                        .await
                    {
                        error!(
                            "Failed to update route for group {:?} during sync: {}",
                            group.group_id, e
                        );
                    }
                }

                // Update routing table with all streams from snapshot
                for stream in &snapshot.streams {
                    debug!(
                        "Stream {} assigned to group {:?}",
                        stream.stream_name, stream.group_id
                    );

                    // Create stream route
                    let stream_route = crate::services::routing::types::StreamRoute {
                        stream_name: stream.stream_name.to_string(),
                        group_id: stream.group_id,
                        assigned_at: std::time::SystemTime::now(),
                        strategy: crate::services::routing::types::RoutingStrategy::Sticky,
                        is_active: true,
                        config: Some(stream.config.clone()),
                    };

                    if let Err(e) = self
                        .routing_table
                        .update_stream_route(stream.stream_name.to_string(), stream_route)
                        .await
                    {
                        error!(
                            "Failed to create stream route for {} during sync: {}",
                            stream.stream_name, e
                        );
                    }
                }
            }

            GlobalConsensusEvent::GroupCreated { group_id, members } => {
                // Synchronously update routing table when a group is created

                // Determine if we're a member of this group
                let is_member = members.contains(&self.local_node_id);
                let location = if is_member {
                    GroupLocation::Local
                } else {
                    GroupLocation::Remote
                };

                info!(
                    "GlobalConsensusSubscriber: {:?} Group {:?} created with {} members",
                    location,
                    group_id,
                    members.len(),
                );

                // Create the group route
                let route = GroupRoute {
                    group_id,
                    location,
                    members: members.clone(),
                    leader: None, // Leader will be set by group consensus events
                    stream_count: 0,
                    health: crate::services::routing::types::GroupHealth::Healthy,
                    last_updated: std::time::SystemTime::now(),
                };

                // Update the routing table
                if let Err(e) = self.routing_table.update_group_route(group_id, route).await {
                    error!("Failed to update route for new group {:?}: {}", group_id, e);
                } else {
                    info!(
                        "Added route for group {:?} (location: {:?})",
                        group_id, location
                    );
                }
            }

            GlobalConsensusEvent::GroupDissolved { group_id } => {
                // Remove group from routing table
                info!("GlobalConsensusSubscriber: Group {:?} dissolved", group_id);

                if let Err(e) = self.routing_table.remove_group_route(group_id).await {
                    error!(
                        "Failed to remove route for dissolved group {:?}: {}",
                        group_id, e
                    );
                }
            }

            GlobalConsensusEvent::StreamCreated {
                stream_name,
                config,
                group_id,
            } => {
                // Update stream count for the group
                debug!(
                    "GlobalConsensusSubscriber: Stream {} created in group {:?}",
                    stream_name, group_id
                );

                // Create stream route
                let stream_route = crate::services::routing::types::StreamRoute {
                    stream_name: stream_name.to_string(),
                    group_id,
                    assigned_at: std::time::SystemTime::now(),
                    strategy: crate::services::routing::types::RoutingStrategy::Sticky,
                    is_active: true,
                    config: Some(config),
                };

                if let Err(e) = self
                    .routing_table
                    .update_stream_route(stream_name.to_string(), stream_route)
                    .await
                {
                    error!("Failed to create stream route for {}: {}", stream_name, e);
                }

                // Update stream count for the group
                if let Ok(Some(mut route)) = self.routing_table.get_group_route(group_id).await {
                    route.stream_count += 1;
                    route.last_updated = std::time::SystemTime::now();

                    if let Err(e) = self.routing_table.update_group_route(group_id, route).await {
                        error!(
                            "Failed to update stream count for group {:?}: {}",
                            group_id, e
                        );
                    }
                }
            }

            GlobalConsensusEvent::StreamDeleted { stream_name } => {
                debug!("GlobalConsensusSubscriber: Stream {} deleted", stream_name);

                // Get the stream route to find which group it belonged to
                if let Ok(Some(stream_route)) = self
                    .routing_table
                    .get_stream_route(stream_name.as_str())
                    .await
                {
                    let group_id = stream_route.group_id;

                    // Remove the stream route
                    if let Err(e) = self
                        .routing_table
                        .remove_stream_route(stream_name.as_str())
                        .await
                    {
                        error!("Failed to remove stream route for {}: {}", stream_name, e);
                    }

                    // Update stream count for the group
                    if let Ok(Some(mut route)) = self.routing_table.get_group_route(group_id).await
                    {
                        route.stream_count = route.stream_count.saturating_sub(1);
                        route.last_updated = std::time::SystemTime::now();

                        if let Err(e) = self.routing_table.update_group_route(group_id, route).await
                        {
                            error!(
                                "Failed to update stream count for group {:?}: {}",
                                group_id, e
                            );
                        }
                    }
                }
            }

            GlobalConsensusEvent::MembershipChanged {
                added_members,
                removed_members,
            } => {
                // Update group memberships
                info!(
                    "GlobalConsensusSubscriber: Membership changed - added: {:?}, removed: {:?}",
                    added_members, removed_members
                );

                // This might affect multiple groups, so we'd need to query which groups
                // are affected and update their routes accordingly
                // For now, we'll just log it as the group-specific events will handle updates
            }

            GlobalConsensusEvent::LeaderChanged { new_leader, .. } => {
                // Update global leader in routing table
                if let Some(leader) = new_leader {
                    self.routing_table
                        .update_global_leader(Some(leader.clone()))
                        .await;
                    info!(
                        "GlobalConsensusSubscriber: Updated global leader to {}",
                        leader
                    );
                }
            }
        }
    }
}
