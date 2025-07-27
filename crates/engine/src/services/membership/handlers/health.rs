//! Handler for health check requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::trace;

use crate::{
    error::ConsensusResult,
    foundation::{NodeRole, NodeStatus},
    services::membership::{
        MembershipView,
        messages::{GlobalConsensusInfo, HealthCheckRequest, HealthCheckResponse},
        utils::now_timestamp,
    },
};

/// Handles health check requests
pub struct HealthCheckHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
}

impl HealthCheckHandler {
    /// Create a new health check handler
    pub fn new(node_id: NodeId, membership_view: Arc<RwLock<MembershipView>>) -> Self {
        Self {
            node_id,
            membership_view,
        }
    }

    /// Handle a health check request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: HealthCheckRequest,
    ) -> ConsensusResult<HealthCheckResponse> {
        trace!(
            "Received health check from {} (seq: {})",
            sender, request.sequence
        );

        // Get global consensus info from membership view
        let view = self.membership_view.read().await;

        // Check if we're a member of global consensus
        let is_member = view
            .nodes
            .get(&self.node_id)
            .map(|node| node.roles.contains(&NodeRole::GlobalConsensusMember))
            .unwrap_or(false);

        // Find the current leader
        let current_leader = view
            .nodes_with_role(&NodeRole::GlobalConsensusLeader)
            .first()
            .map(|member| member.node_id.clone());

        drop(view);

        let global_consensus_info = if is_member || current_leader.is_some() {
            Some(GlobalConsensusInfo {
                is_member,
                current_leader,
                current_term: 0, // TODO: Get from consensus service
            })
        } else {
            None
        };

        let response = HealthCheckResponse {
            status: NodeStatus::Online, // TODO: Get actual status from service state
            load: None,                 // TODO: Implement load tracking
            global_consensus_info,
            timestamp: now_timestamp(),
        };

        Ok(response)
    }
}
