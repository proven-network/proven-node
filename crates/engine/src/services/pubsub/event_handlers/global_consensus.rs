//! Global consensus event subscriber for PubSub service
//!
//! Handles global consensus membership changes to maintain cluster awareness

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::foundation::events::{EventBus, EventHandler, EventMetadata};
use crate::services::global_consensus::events::GlobalConsensusEvent;
use crate::services::pubsub::PubSubService;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Subscriber for global consensus events that affect PubSub
#[derive(Clone)]
pub struct GlobalConsensusEventSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
    event_bus: Arc<EventBus>,
}

impl<T, G> GlobalConsensusEventSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new global consensus event subscriber
    pub fn new(service: Arc<PubSubService<T, G>>, event_bus: Arc<EventBus>) -> Self {
        Self { service, event_bus }
    }
}

#[async_trait]
impl<T, G> EventHandler<GlobalConsensusEvent> for GlobalConsensusEventSubscriber<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(&self, event: GlobalConsensusEvent, _metadata: EventMetadata) {
        match event {
            GlobalConsensusEvent::MembershipChanged {
                added_members,
                removed_members,
            } => {
                info!(
                    "PubSubService: Global consensus membership changed - {} added, {} removed",
                    added_members.len(),
                    removed_members.len()
                );

                // Add new members to cluster members list
                for node_id in &added_members {
                    self.service.add_cluster_member(node_id).await;
                }

                // Remove members from cluster members list
                for node_id in &removed_members {
                    self.service.remove_node_interests(node_id).await;
                }

                // Send our current interests to newly added members
                if !added_members.is_empty()
                    && let Err(e) = self
                        .service
                        .broadcast_interests_to_nodes(&added_members)
                        .await
                {
                    debug!("Failed to broadcast interests to new members: {}", e);
                }
            }

            _ => {
                // Ignore other global consensus events
            }
        }
    }
}
