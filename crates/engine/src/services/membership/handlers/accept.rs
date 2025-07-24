//! Handler for accept proposal requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::{
    error::ConsensusResult,
    services::membership::{
        MembershipView,
        messages::{AcceptProposalRequest, AcceptProposalResponse},
    },
};

/// Handles accept proposal requests
pub struct AcceptProposalHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
}

impl AcceptProposalHandler {
    /// Create a new accept proposal handler
    pub fn new(node_id: NodeId, membership_view: Arc<RwLock<MembershipView>>) -> Self {
        Self {
            node_id,
            membership_view,
        }
    }

    /// Handle an accept proposal request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: AcceptProposalRequest,
    ) -> ConsensusResult<AcceptProposalResponse> {
        info!(
            "Received proposal acceptance from {} for formation_id: {}",
            sender, request.formation_id
        );

        // TODO: Implement proper acceptance handling
        // This would involve:
        // 1. Verifying the formation_id matches our current forming state
        // 2. Recording the acceptance
        // 3. Checking if we have enough acceptances to form the cluster
        // 4. If we're the coordinator and have enough acceptances, initiate cluster formation

        let view = self.membership_view.read().await;
        let cluster_state = view.cluster_state.clone();
        drop(view);

        debug!(
            "Current cluster state: {:?}, acceptance from: {}",
            cluster_state, sender
        );

        Ok(AcceptProposalResponse {
            success: true,
            cluster_state,
        })
    }
}
