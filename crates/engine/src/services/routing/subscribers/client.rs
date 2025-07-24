//! Client service event subscriber for routing service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::services::client::events::ClientServiceEvent;
use crate::services::event::{EventHandler, EventPriority};
use crate::services::routing::RoutingTable;
use proven_topology::NodeId;

/// Subscriber for client service events that updates routing information
#[derive(Clone)]
pub struct ClientServiceSubscriber {
    routing_table: Arc<RoutingTable>,
    local_node_id: NodeId,
}

impl ClientServiceSubscriber {
    /// Create a new client service subscriber
    pub fn new(routing_table: Arc<RoutingTable>, local_node_id: NodeId) -> Self {
        Self {
            routing_table,
            local_node_id,
        }
    }
}

#[async_trait]
impl EventHandler<ClientServiceEvent> for ClientServiceSubscriber {
    fn priority(&self) -> EventPriority {
        // Handle ClientServiceEvents synchronously
        EventPriority::Critical
    }

    async fn handle(&self, event: ClientServiceEvent) {
        match event {
            ClientServiceEvent::LearnedStreamExists {
                stream_name,
                group_id,
            } => {
                // We learned about an existing stream (e.g., from StreamAlreadyExists response)
                info!(
                    "ClientServiceSubscriber: Learned about existing stream {} in group {:?}",
                    stream_name, group_id
                );

                // Create stream route
                let stream_route = crate::services::routing::types::StreamRoute {
                    stream_name: stream_name.to_string(),
                    group_id,
                    assigned_at: std::time::SystemTime::now(),
                    strategy: crate::services::routing::types::RoutingStrategy::Sticky,
                    is_active: true,
                    config: None, // Config not available from StreamAlreadyExists response
                };

                // Add stream route to routing table
                if let Err(e) = self
                    .routing_table
                    .update_stream_route(stream_name.to_string(), stream_route)
                    .await
                {
                    error!(
                        "Failed to add route for learned stream {}: {}",
                        stream_name, e
                    );
                } else {
                    debug!(
                        "Added route for learned stream {} -> group {:?}",
                        stream_name, group_id
                    );

                    // Update stream count for the group
                    if let Ok(Some(mut route)) = self.routing_table.get_group_route(group_id).await
                    {
                        route.stream_count += 1;
                        route.last_updated = std::time::SystemTime::now();

                        if let Err(e) = self.routing_table.update_group_route(group_id, route).await
                        {
                            warn!(
                                "Failed to update stream count for group {:?}: {}",
                                group_id, e
                            );
                        }
                    }
                }
            }

            // Ignore PubSub-related events (handled by PubSub service)
            ClientServiceEvent::PubSubPublish { .. }
            | ClientServiceEvent::PubSubSubscribe { .. }
            | ClientServiceEvent::PubSubUnsubscribe { .. } => {
                // These events are handled by the PubSub service subscriber
            }
        }
    }
}
