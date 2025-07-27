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
use crate::foundation::types::SubjectPattern;
use crate::services::membership::events::MembershipEvent;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::messages::PubSubServiceMessage;
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
    ) -> Self {
        Self {
            node_id,
            interest_tracker,
            message_router,
            network,
            cluster_members,
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

    /// Broadcast current interests to specific nodes
    async fn broadcast_interests_to_nodes(&self, nodes: &[NodeId]) -> Result<(), String> {
        let subscriptions = self
            .message_router
            .get_node_subscriptions(&self.node_id)
            .await;

        if subscriptions.is_empty() {
            return Ok(());
        }

        let patterns: Vec<SubjectPattern> = subscriptions
            .into_iter()
            .map(|sub| sub.subject_pattern)
            .collect();

        let msg = PubSubServiceMessage::RegisterInterest {
            patterns,
            timestamp: std::time::SystemTime::now(),
        };

        // Send to specified nodes
        for node in nodes {
            if node != &self.node_id
                && let Err(e) = self
                    .network
                    .request_with_timeout::<PubSubServiceMessage>(
                        node.clone(),
                        msg.clone(),
                        std::time::Duration::from_millis(100),
                    )
                    .await
            {
                debug!("Failed to send interests to node {}: {}", node, e);
            }
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
                info!(
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
                info!(
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
                info!(
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
                info!(
                    "PubSubService: Node {} went offline, removing interests",
                    node_id
                );

                // Remove the node's interests from our tracker
                self.remove_node_interests(&node_id).await;
            }

            _ => {
                // Ignore other membership events
            }
        }
    }
}
