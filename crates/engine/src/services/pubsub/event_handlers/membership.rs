//! Membership event subscriber for PubSub service
//!
//! Handles membership events to maintain awareness of cluster nodes for interest propagation

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::foundation::events::{EventBus, EventHandler, EventMetadata};
use crate::services::membership::events::MembershipEvent;
use crate::services::pubsub::PubSubService;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Subscriber for membership events that affect PubSub interest propagation
#[derive(Clone)]
pub struct MembershipEventSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
    event_bus: Arc<EventBus>,
}

impl<T, G> MembershipEventSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new membership event subscriber
    pub fn new(service: Arc<PubSubService<T, G>>, event_bus: Arc<EventBus>) -> Self {
        Self { service, event_bus }
    }
}

#[async_trait]
impl<T, G> EventHandler<MembershipEvent> for MembershipEventSubscriber<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(&self, event: MembershipEvent, _metadata: EventMetadata) {
        match event {
            MembershipEvent::ClusterFormed { members, .. } => {
                info!(
                    "PubSubService: Cluster formed with {} members, propagating interests",
                    members.len()
                );

                // Update cluster members list
                self.service.update_cluster_members(&members).await;

                // Send our current interests to all members
                if let Err(e) = self.service.broadcast_interests_to_nodes(&members).await {
                    debug!("Failed to broadcast interests on cluster formation: {}", e);
                }
            }

            MembershipEvent::ClusterJoined { members, .. } => {
                info!(
                    "PubSubService: Joined cluster with {} members, propagating interests",
                    members.len()
                );

                // Update cluster members list
                self.service.update_cluster_members(&members).await;

                // Send our current interests to all members when we join a cluster
                if let Err(e) = self.service.broadcast_interests_to_nodes(&members).await {
                    debug!("Failed to broadcast interests after joining cluster: {}", e);
                }
            }

            MembershipEvent::NodeDiscovered { node_id, .. } => {
                info!(
                    "PubSubService: Node {} discovered, sending interests",
                    node_id
                );

                // Add to cluster members
                self.service.add_cluster_member(&node_id).await;

                // Send our current interests to the new node
                if let Err(e) = self.service.broadcast_interests_to_nodes(&[node_id]).await {
                    debug!("Failed to send interests to discovered node: {}", e);
                }
            }

            MembershipEvent::NodeOffline { node_id, .. } => {
                info!(
                    "PubSubService: Node {} went offline, removing interests",
                    node_id
                );

                // Remove the node's interests from our tracker
                self.service.remove_node_interests(&node_id).await;
            }

            _ => {
                // Ignore other membership events
            }
        }
    }
}
