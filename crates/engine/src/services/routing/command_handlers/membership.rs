//! Membership event subscriber for routing service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::info;

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::services::membership::events::MembershipEvent;
use crate::services::routing::RoutingTable;

/// Subscriber for membership events that updates routing information
#[derive(Clone)]
pub struct MembershipSubscriber {
    routing_table: Arc<RoutingTable>,
}

impl MembershipSubscriber {
    /// Create a new membership subscriber
    pub fn new(routing_table: Arc<RoutingTable>) -> Self {
        Self { routing_table }
    }
}

#[async_trait]
impl EventHandler<MembershipEvent> for MembershipSubscriber {
    async fn handle(&self, event: MembershipEvent, _metadata: EventMetadata) {
        match event {
            MembershipEvent::ClusterFormed {
                coordinator,
                members,
            } => {
                // For a single-node cluster, the coordinator is also the global consensus leader
                info!(
                    "MembershipSubscriber: Cluster formed with coordinator {} and {} members",
                    coordinator,
                    members.len()
                );

                // Update global leader to the coordinator
                // This handles the initial leader assignment for single-node clusters
                self.routing_table
                    .update_global_leader(Some(coordinator.clone()))
                    .await;

                info!(
                    "MembershipSubscriber: Set initial global leader to coordinator {coordinator}"
                );
            }

            MembershipEvent::ClusterJoined { leader, members } => {
                // When joining an existing cluster, update the global leader
                info!(
                    "MembershipSubscriber: Joined cluster with leader {} and {} members",
                    leader,
                    members.len()
                );

                self.routing_table
                    .update_global_leader(Some(leader.clone()))
                    .await;

                info!("MembershipSubscriber: Set global leader to {leader}");
            }

            // Other membership events don't affect global leader routing
            _ => {}
        }
    }
}
