//! Handler for propose cluster requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::{
    error::ConsensusResult,
    foundation::ClusterFormationState,
    services::membership::{
        MembershipView,
        messages::{ProposeClusterRequest, ProposeClusterResponse},
    },
};

/// Handles propose cluster requests
pub struct ProposeClusterHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
}

impl ProposeClusterHandler {
    /// Create a new propose cluster handler
    pub fn new(node_id: NodeId, membership_view: Arc<RwLock<MembershipView>>) -> Self {
        Self {
            node_id,
            membership_view,
        }
    }

    /// Handle a propose cluster request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: ProposeClusterRequest,
    ) -> ConsensusResult<ProposeClusterResponse> {
        debug!(
            "Received cluster proposal from {} (formation_id: {}, members: {:?})",
            sender,
            request.formation_id,
            request
                .proposed_members
                .iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        );

        // Check if we're already in a cluster
        let view = self.membership_view.read().await;
        let accepted = matches!(view.cluster_state, ClusterFormationState::NotFormed);
        drop(view);

        if accepted {
            debug!(
                "Accepting cluster proposal from {} (formation_id: {})",
                sender, request.formation_id
            );

            // Update our state to forming
            let mut view = self.membership_view.write().await;
            view.cluster_state = ClusterFormationState::Forming {
                coordinator: request.coordinator.clone(),
                formation_id: request.formation_id,
                proposed_members: request
                    .proposed_members
                    .iter()
                    .map(|(id, _)| id.clone())
                    .collect(),
            };
            drop(view);

            Ok(ProposeClusterResponse {
                accepted: true,
                rejection_reason: None,
            })
        } else {
            warn!(
                "Rejecting cluster proposal from {} - already in a cluster",
                sender
            );

            Ok(ProposeClusterResponse {
                accepted: false,
                rejection_reason: Some("Already in cluster".to_string()),
            })
        }
    }
}
