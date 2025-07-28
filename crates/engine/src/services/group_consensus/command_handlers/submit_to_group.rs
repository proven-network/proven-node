//! Handler for SubmitToGroup command

use async_trait::async_trait;
use std::sync::Arc;

use crate::consensus::group::types::{GroupRequest, GroupResponse};
use crate::foundation::{
    events::{Error as EventError, EventMetadata, RequestHandler},
    routing::RoutingTable,
    types::ConsensusGroupId,
};
use crate::services::group_consensus::commands::SubmitToGroup;
use crate::services::group_consensus::types::ConsensusLayers;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Handler for SubmitToGroup command
pub struct SubmitToGroupHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
    /// Routing table for finding group members
    routing_table: Arc<RoutingTable>,
    /// Network manager for forwarding requests
    network_manager: Arc<NetworkManager<T, G, A>>,
}

impl<T, G, A, S> SubmitToGroupHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(
        node_id: NodeId,
        groups: ConsensusLayers<S>,
        routing_table: Arc<RoutingTable>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> Self {
        Self {
            node_id,
            groups,
            routing_table,
            network_manager,
        }
    }

    /// Forward a group request to another node
    async fn forward_to_node(
        &self,
        group_id: ConsensusGroupId,
        target: NodeId,
        request: GroupRequest,
    ) -> Result<GroupResponse, EventError> {
        use crate::services::group_consensus::messages::{
            GroupConsensusMessage, GroupConsensusServiceResponse,
        };

        let message = GroupConsensusMessage::Consensus { group_id, request };

        match self
            .network_manager
            .request_with_timeout(target, message, std::time::Duration::from_secs(30))
            .await
        {
            Ok(GroupConsensusServiceResponse::Consensus { response, .. }) => Ok(response),
            Ok(_) => Err(EventError::Internal(
                "Unexpected response type from remote node".to_string(),
            )),
            Err(e) => Err(EventError::Internal(format!("Network error: {e}"))),
        }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<SubmitToGroup> for SubmitToGroupHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: SubmitToGroup,
        _metadata: EventMetadata,
    ) -> Result<GroupResponse, EventError> {
        // Check if we're a member of this group
        let groups_guard = self.groups.read().await;
        if let Some(group) = groups_guard.get(&request.group_id) {
            // We're a member - handle locally
            match group.submit_request(request.request.clone()).await {
                Ok(response) => Ok(response),
                Err(e) if e.is_not_leader() => {
                    // We're not the leader - forward to the leader
                    if let Some(leader) = e.get_leader() {
                        drop(groups_guard); // Release lock before network call
                        self.forward_to_node(request.group_id, leader.clone(), request.request)
                            .await
                    } else {
                        Err(EventError::Internal(format!("Not leader: {e}")))
                    }
                }
                Err(e) => Err(EventError::Internal(e.to_string())),
            }
        } else {
            drop(groups_guard); // Release lock before network call

            // Not a member of this group - forward to a member
            let route = self
                .routing_table
                .get_group_route(request.group_id)
                .await
                .map_err(|e| EventError::Internal(format!("Routing error: {e}")))?
                .ok_or_else(|| {
                    EventError::Internal(format!(
                        "Group {:?} not found in routing table",
                        request.group_id
                    ))
                })?;

            if let Some(member) = route.members.first() {
                self.forward_to_node(request.group_id, member.clone(), request.request)
                    .await
            } else {
                Err(EventError::Internal(format!(
                    "Group {:?} has no members",
                    request.group_id
                )))
            }
        }
    }
}
