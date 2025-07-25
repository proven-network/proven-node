//! Handler for graceful shutdown requests

use std::sync::Arc;

use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    error::ConsensusResult,
    foundation::events::EventBus,
    services::membership::{
        MembershipView,
        events::MembershipEvent,
        messages::{GracefulShutdownRequest, GracefulShutdownResponse},
        types::NodeStatus,
        utils::now_timestamp,
    },
};

/// Handles graceful shutdown requests
pub struct GracefulShutdownHandler {
    /// Node ID
    node_id: NodeId,
    /// Membership view
    membership_view: Arc<RwLock<MembershipView>>,
    /// Event bus for publishing events
    event_bus: Arc<EventBus>,
}

impl GracefulShutdownHandler {
    /// Create a new graceful shutdown handler
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

    /// Handle a graceful shutdown request
    pub async fn handle(
        &self,
        sender: NodeId,
        request: GracefulShutdownRequest,
    ) -> ConsensusResult<GracefulShutdownResponse> {
        info!(
            "Node {} announced graceful shutdown: {:?}",
            sender, request.reason
        );

        // Immediately mark the node as offline
        let mut view = self.membership_view.write().await;
        if let Some(member) = view.nodes.get_mut(&sender) {
            member.status = NodeStatus::Offline {
                since_ms: now_timestamp(),
            };
            info!("Marked node {} as offline due to graceful shutdown", sender);
        }
        drop(view);

        // Publish event for immediate membership change
        self.event_bus.emit(MembershipEvent::NodeGracefulShutdown {
            node_id: sender.clone(),
            reason: request.reason.clone(),
        });

        // Also publish membership change required event
        self.event_bus
            .emit(MembershipEvent::MembershipChangeRequired {
                add_nodes: vec![],
                remove_nodes: vec![sender.clone()],
                reason: format!("Node {sender} gracefully shutting down"),
            });

        // Update global consensus membership to remove the node
        info!(
            "Updating global consensus membership to remove node {}",
            sender
        );

        use crate::services::global_consensus::commands::RemoveNodeFromConsensus;
        let remove_node_cmd = RemoveNodeFromConsensus {
            node_id: sender.clone(),
        };

        match self.event_bus.request(remove_node_cmd).await {
            Ok(members) => {
                info!(
                    "Successfully removed node {} from global consensus membership. Current members: {:?}",
                    sender, members
                );
            }
            Err(e) => {
                warn!(
                    "Failed to remove node {} from global consensus membership: {}",
                    sender, e
                );
                // Continue anyway - the membership monitor will retry
            }
        }

        Ok(GracefulShutdownResponse { acknowledged: true })
    }
}
