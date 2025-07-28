//! Handler for GetStreamState command

use async_trait::async_trait;
use std::sync::Arc;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::foundation::models::stream::StreamState;
use crate::foundation::{GroupStateRead, RoutingTable, StreamName};
use crate::services::group_consensus::commands::GetStreamState;
use crate::services::group_consensus::messages::{
    GroupConsensusMessage, GroupConsensusServiceResponse,
};
use crate::services::group_consensus::types::{ConsensusLayers, States};
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Handler for GetStreamState command
pub struct GetStreamStateHandler<T, G, A, S>
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
    /// Map of group IDs to state readers
    group_states: States,
    /// Routing table for finding group members
    routing_table: Arc<RoutingTable>,
    /// Network manager for forwarding requests
    network_manager: Arc<NetworkManager<T, G, A>>,
}

impl<T, G, A, S> GetStreamStateHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(
        node_id: NodeId,
        groups: ConsensusLayers<S>,
        group_states: States,
        routing_table: Arc<RoutingTable>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> Self {
        Self {
            node_id,
            groups,
            group_states,
            routing_table,
            network_manager,
        }
    }

    /// Forward request to a node in the group
    async fn forward_to_node(
        &self,
        target: NodeId,
        request: GetStreamState,
    ) -> Result<Option<StreamState>, EventError> {
        let message = GroupConsensusMessage::GetStreamState {
            group_id: request.group_id,
            stream_name: request.stream_name.clone(),
        };

        match self
            .network_manager
            .request_with_timeout(target.clone(), message, std::time::Duration::from_secs(5))
            .await
        {
            Ok(GroupConsensusServiceResponse::StreamState(state)) => Ok(state),
            Ok(GroupConsensusServiceResponse::Error(e)) => Err(EventError::Internal(e)),
            Ok(_) => Err(EventError::Internal("Unexpected response type".to_string())),
            Err(e) => Err(EventError::Internal(format!(
                "Failed to forward request: {e}"
            ))),
        }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetStreamState> for GetStreamStateHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetStreamState,
        _metadata: EventMetadata,
    ) -> Result<Option<StreamState>, EventError> {
        // Check if we have this group locally
        let is_local = self
            .routing_table
            .is_group_local(request.group_id)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to check group location: {e}")))?;

        if is_local {
            // Handle locally
            let states_guard = self.group_states.read().await;
            if let Some(state) = states_guard.get(&request.group_id) {
                let stream_name = StreamName::new(request.stream_name.as_str());
                Ok(state.get_stream(&stream_name).await)
            } else {
                Ok(None)
            }
        } else {
            // Forward to a member of the group
            let route = self
                .routing_table
                .get_group_route(request.group_id)
                .await
                .map_err(|e| EventError::Internal(format!("Failed to get group route: {e}")))?;

            if let Some(route) = route {
                if let Some(member) = route.members.first() {
                    self.forward_to_node(member.clone(), request).await
                } else {
                    Err(EventError::Internal(format!(
                        "Group {:?} has no members",
                        request.group_id
                    )))
                }
            } else {
                Ok(None)
            }
        }
    }
}
