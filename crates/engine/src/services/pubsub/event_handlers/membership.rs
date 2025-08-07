//! Membership event subscriber for PubSub service
//!
//! Handles membership events to maintain awareness of cluster nodes for interest propagation

use async_trait::async_trait;
use proven_attestation::Attestor;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::services::membership::events::MembershipEvent;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::streaming_router::StreamingMessageRouter;
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Subscriber for membership events that affect PubSub interest propagation
#[derive(Clone)]
pub struct MembershipEventSubscriber<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    node_id: NodeId,
    interest_tracker: InterestTracker,
    message_router: StreamingMessageRouter,
    network: Arc<NetworkManager<T, G, A>>,
    cluster_members: Arc<RwLock<HashSet<NodeId>>>,
    interest_propagator: Arc<super::super::internal::InterestPropagator>,
}

impl<T, G, A> MembershipEventSubscriber<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Create a new membership event subscriber
    pub fn new(
        node_id: NodeId,
        interest_tracker: InterestTracker,
        message_router: StreamingMessageRouter,
        network: Arc<NetworkManager<T, G, A>>,
        cluster_members: Arc<RwLock<HashSet<NodeId>>>,
        interest_propagator: Arc<super::super::internal::InterestPropagator>,
    ) -> Self {
        Self {
            node_id,
            interest_tracker,
            message_router,
            network,
            cluster_members,
            interest_propagator,
        }
    }

    /// Update cluster members list
    async fn update_cluster_members(&self, members: &[NodeId]) {
        let mut cluster_members = self.cluster_members.write().await;
        cluster_members.clear();
        cluster_members.extend(members.iter().cloned());
    }

    /// Add a cluster member
    async fn add_cluster_member(&self, node_id: &NodeId) {
        self.cluster_members.write().await.insert(node_id.clone());
    }

    /// Remove a node's interests from the tracker
    async fn remove_node_interests(&self, node_id: &NodeId) {
        self.interest_tracker.remove_node(node_id).await;
        self.cluster_members.write().await.remove(node_id);
    }

    /// Broadcast current interests to specific nodes via control streams
    async fn broadcast_interests_to_nodes(&self, _nodes: &[NodeId]) -> Result<(), String> {
        if let Err(e) = self.interest_propagator.propagate_interests().await {
            debug!("Failed to propagate interests: {}", e);
        }
        Ok(())
    }
}

#[async_trait]
impl<T, G, A> EventHandler<MembershipEvent> for MembershipEventSubscriber<T, G, A>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
{
    async fn handle(&self, event: MembershipEvent, _metadata: EventMetadata) {
        match event {
            MembershipEvent::ClusterFormed { members, .. } => {
                debug!(
                    "PubSubService: Cluster formed with {} members, propagating interests",
                    members.len()
                );

                // Update cluster members list
                self.update_cluster_members(&members).await;

                // Send our current interests to all members
                if let Err(e) = self.broadcast_interests_to_nodes(&members).await {
                    debug!("Failed to broadcast interests on cluster formation: {}", e);
                }
            }

            MembershipEvent::ClusterJoined { members, .. } => {
                debug!(
                    "PubSubService: Joined cluster with {} members, propagating interests",
                    members.len()
                );

                // Update cluster members list
                self.update_cluster_members(&members).await;

                // Send our current interests to all members when we join a cluster
                if let Err(e) = self.broadcast_interests_to_nodes(&members).await {
                    debug!("Failed to broadcast interests after joining cluster: {}", e);
                }
            }

            MembershipEvent::NodeDiscovered { node_id, .. } => {
                debug!(
                    "PubSubService: Node {} discovered, sending interests",
                    node_id
                );

                // Add to cluster members
                self.add_cluster_member(&node_id).await;

                // Send our current interests to the new node
                if let Err(e) = self.broadcast_interests_to_nodes(&[node_id]).await {
                    debug!("Failed to send interests to discovered node: {}", e);
                }
            }

            MembershipEvent::NodeOffline { node_id, .. } => {
                debug!(
                    "PubSubService: Node {} went offline, removing interests",
                    node_id
                );

                // Remove the node's interests from our tracker
                self.remove_node_interests(&node_id).await;
            }

            MembershipEvent::MembershipChangeRequired { add_nodes, .. } => {
                debug!(
                    "PubSubService: Membership change required, adding {} nodes",
                    add_nodes.len()
                );

                // Add new nodes to cluster members
                let new_nodes: Vec<NodeId> = add_nodes
                    .iter()
                    .map(|(node_id, _)| node_id.clone())
                    .collect();
                for node_id in &new_nodes {
                    self.add_cluster_member(node_id).await;
                }

                // Propagate interests to new nodes
                if !new_nodes.is_empty()
                    && let Err(e) = self.broadcast_interests_to_nodes(&new_nodes).await
                {
                    debug!("Failed to send interests to new nodes: {}", e);
                }
            }

            _ => {
                // Ignore other membership events
            }
        }
    }
}
