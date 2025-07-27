//! Handler for discover cluster requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;

use crate::{
    error::ConsensusResult,
    foundation::{NodeRole, NodeStatus},
    services::membership::{
        MembershipView,
        messages::{ClusterState, DiscoverClusterRequest, DiscoverClusterResponse},
        utils::now_timestamp,
    },
};

/// Handles discover cluster requests
pub struct DiscoverClusterHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
}

impl DiscoverClusterHandler {
    /// Create a new discover cluster handler
    pub fn new(node_id: NodeId, membership_view: Arc<RwLock<MembershipView>>) -> Self {
        Self {
            node_id,
            membership_view,
        }
    }

    /// Handle a discover cluster request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: DiscoverClusterRequest,
    ) -> ConsensusResult<DiscoverClusterResponse> {
        use tracing::info;

        info!(
            "Received discover cluster request from {} (round: {})",
            sender, request.round_id
        );

        let view = self.membership_view.read().await;

        // Log our current cluster state
        info!(
            "Current cluster state: {:?}, nodes: {}, with roles: {:?}",
            view.cluster_state,
            view.nodes.len(),
            view.nodes
                .values()
                .map(|n| (&n.node_id, &n.roles))
                .collect::<Vec<_>>()
        );

        // Convert cluster state and find the leader
        let mut cluster_state: ClusterState = view.cluster_state.clone().into();

        // If we have an active cluster, find who has the GlobalConsensusLeader role
        if let ClusterState::Active {
            ref mut leader,
            ref members,
            term,
            ..
        } = cluster_state
        {
            *leader = view
                .nodes_with_role(&NodeRole::GlobalConsensusLeader)
                .first()
                .map(|member| member.node_id.clone());

            info!(
                "Reporting active cluster: leader={:?}, members={}, term={}",
                leader,
                members.len(),
                term
            );
            // TODO: Get actual term and committed index from consensus
            // For now, these remain as 0
        }

        let response = DiscoverClusterResponse {
            from_node: self.node_id.clone(),
            cluster_state,
            node_status: NodeStatus::Online, // TODO: Get from service state
            timestamp: now_timestamp(),
        };

        info!(
            "Sending discover response to {}: cluster_state={:?}",
            sender, response.cluster_state
        );

        Ok(response)
    }
}
