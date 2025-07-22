//! Group consensus event subscriber for routing service

use async_trait::async_trait;
use proven_logger::{debug, error, info};
use std::sync::Arc;

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{EventHandler, EventPriority};
use crate::services::group_consensus::events::GroupConsensusEvent;
use crate::services::routing::{GroupLocation, RoutingTable};
use proven_topology::NodeId;

/// Subscriber for group consensus events that updates routing information
#[derive(Clone)]
pub struct GroupConsensusSubscriber {
    routing_table: Arc<RoutingTable>,
    local_node_id: NodeId,
}

impl GroupConsensusSubscriber {
    /// Create a new group consensus subscriber
    pub fn new(routing_table: Arc<RoutingTable>, local_node_id: NodeId) -> Self {
        Self {
            routing_table,
            local_node_id,
        }
    }
}

#[async_trait]
impl EventHandler<GroupConsensusEvent> for GroupConsensusSubscriber {
    fn priority(&self) -> EventPriority {
        // Handle GroupConsensusEvents synchronously
        EventPriority::Critical
    }

    async fn handle(&self, event: GroupConsensusEvent) {
        match event {
            GroupConsensusEvent::StateSynchronized { group_id } => {
                // This is a critical synchronous event
                debug!("GroupConsensusSubscriber: Group {group_id:?} state synchronized");
            }

            GroupConsensusEvent::StreamCreated {
                group_id,
                stream_name,
            } => {
                // Synchronously handle stream creation
                debug!(
                    "GroupConsensusSubscriber: Stream {stream_name} created in group {group_id:?}"
                );

                // The routing table should already have this info from global consensus
                // This is more for confirmation and local tracking
            }

            GroupConsensusEvent::StreamRemoved {
                group_id,
                stream_name,
            } => {
                // Synchronously handle stream removal
                debug!(
                    "GroupConsensusSubscriber: Stream {stream_name} removed from group {group_id:?}"
                );
            }

            GroupConsensusEvent::MessagesAppended(data) => {
                let group_id = data.group_id;
                let stream_name = data.stream_name;
                let message_count = data.message_count;
                // This can be async - just for metrics/monitoring
                debug!(
                    "GroupConsensusSubscriber: {message_count} messages appended to {stream_name} in group {group_id:?}"
                );
            }

            GroupConsensusEvent::MembershipChanged(data) => {
                let group_id = data.group_id;
                let added_members = data.added_members;
                let removed_members = data.removed_members;
                // Update group membership in routing table
                info!(
                    "GroupConsensusSubscriber: Group {group_id:?} membership changed - added: {added_members:?}, removed: {removed_members:?}"
                );

                if let Ok(Some(mut route)) = self.routing_table.get_group_route(group_id).await {
                    // Update members list
                    for member in &removed_members {
                        route.members.retain(|m| m != member);
                    }
                    for member in &added_members {
                        if !route.members.contains(member) {
                            route.members.push(member.clone());
                        }
                    }

                    // Check if we're still a member
                    let was_local = matches!(
                        route.location,
                        GroupLocation::Local | GroupLocation::Distributed
                    );
                    let is_member = route.members.contains(&self.local_node_id);

                    // Update location if membership status changed
                    if was_local && !is_member {
                        // We were removed from the group
                        route.location = if route.members.is_empty() {
                            error!("Group {group_id:?} has no members after update");
                            GroupLocation::Remote // Fallback
                        } else {
                            GroupLocation::Remote
                        };
                    } else if !was_local && is_member {
                        // We were added to the group
                        route.location = GroupLocation::Local;
                    }

                    route.last_updated = std::time::SystemTime::now();

                    if let Err(e) = self.routing_table.update_group_route(group_id, route).await {
                        error!("Failed to update group {group_id:?} membership: {e}");
                    }
                }
            }

            GroupConsensusEvent::LeaderChanged {
                group_id,
                new_leader,
                ..
            } => {
                // Update group leader
                debug!(
                    "GroupConsensusSubscriber: Group {group_id:?} leader changed to {new_leader:?}"
                );

                // Only track leader for local groups (where we participate in consensus)
                if let Ok(Some(mut route)) = self.routing_table.get_group_route(group_id).await
                    && (route.location == GroupLocation::Local
                        || route.location == GroupLocation::Distributed)
                {
                    route.leader = new_leader.clone();
                    route.last_updated = std::time::SystemTime::now();

                    if let Err(e) = self.routing_table.update_group_route(group_id, route).await {
                        error!("Failed to update group {group_id:?} leader: {e}");
                    } else {
                        info!("Updated leader for group {group_id:?} to {new_leader:?}");
                    }
                }
            }

            GroupConsensusEvent::MessagesToPersist(_) => {
                // This event is handled by StreamService, not RoutingService
            }
        }
    }
}
