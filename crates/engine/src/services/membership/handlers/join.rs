//! Handler for join cluster requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    error::ConsensusResult,
    foundation::{NodeRole, events::EventBus},
    services::membership::{
        MembershipView,
        events::MembershipEvent,
        messages::{ClusterState, JoinClusterRequest, JoinClusterResponse},
    },
};

/// Handles join cluster requests
pub struct JoinClusterHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
    /// Event bus for publishing events
    event_bus: Arc<EventBus>,
}

impl JoinClusterHandler {
    /// Create a new join cluster handler
    pub fn new(
        node_id: NodeId,
        membership_view: Arc<RwLock<MembershipView>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            membership_view,
            event_bus,
        }
    }

    /// Handle a join cluster request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: JoinClusterRequest,
    ) -> ConsensusResult<JoinClusterResponse> {
        info!(
            "Received join cluster request from node {} with info: {:?}",
            sender, request.node_info
        );

        // Check if we're the leader (by checking if we have the GlobalConsensusLeader role)
        let view = self.membership_view.read().await;
        let is_leader = view
            .nodes
            .get(&self.node_id)
            .map(|node| node.roles.contains(&NodeRole::GlobalConsensusLeader))
            .unwrap_or(false);
        drop(view);

        if !is_leader {
            warn!("Not the global consensus leader, cannot handle join request");
            return Ok(JoinClusterResponse {
                accepted: false,
                rejection_reason: Some("Not the global consensus leader".to_string()),
                cluster_state: None,
            });
        }

        // Check if the node is already a member
        let view = self.membership_view.read().await;
        if view.nodes.contains_key(&sender) {
            info!("Node {} is already a member of the cluster", sender);

            // Get current cluster state
            let cluster_state = self.get_cluster_state(&view).await;
            drop(view);

            return Ok(JoinClusterResponse {
                accepted: true,
                rejection_reason: None,
                cluster_state: Some(cluster_state),
            });
        }
        drop(view);

        // Publish MembershipChangeRequired event to add the node
        info!(
            "Publishing membership change to add node {} to cluster",
            sender
        );

        let event = MembershipEvent::MembershipChangeRequired {
            add_nodes: vec![(sender, request.node_info.clone())],
            remove_nodes: vec![],
            reason: format!("Node {sender} requested to join cluster"),
        };

        // Emit event
        self.event_bus.emit(event);

        // Update global consensus membership to include the new node
        info!(
            "Updating global consensus membership to add node {}",
            sender
        );

        use crate::services::global_consensus::commands::AddNodeToConsensus;
        let add_node_cmd = AddNodeToConsensus { node_id: sender };

        match self.event_bus.request(add_node_cmd).await {
            Ok(members) => {
                info!(
                    "Successfully added node {} to global consensus membership. Current members: {:?}",
                    sender, members
                );
            }
            Err(e) => {
                warn!(
                    "Failed to add node {} to global consensus membership: {}",
                    sender, e
                );
                // Continue anyway - the membership monitor will retry
            }
        }

        info!("Node {} join request approved", sender);

        // Get current cluster state for the response
        let view = self.membership_view.read().await;
        let cluster_state = self.get_cluster_state(&view).await;
        drop(view);

        Ok(JoinClusterResponse {
            accepted: true,
            rejection_reason: None,
            cluster_state: Some(cluster_state),
        })
    }

    /// Get current cluster state
    async fn get_cluster_state(&self, view: &MembershipView) -> ClusterState {
        // Get members from membership view
        let members: Vec<NodeId> = view.nodes.keys().cloned().collect();

        // Find the leader
        let leader = view
            .nodes_with_role(&NodeRole::GlobalConsensusLeader)
            .first()
            .map(|member| member.node_id);

        ClusterState::Active {
            leader,
            members,
            term: 0,            // TODO: Get from consensus
            committed_index: 0, // TODO: Get from consensus
        }
    }
}
