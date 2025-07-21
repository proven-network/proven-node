//! Global consensus handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::{
    consensus::global::{GlobalRequest, GlobalResponse},
    error::{ConsensusResult, Error, ErrorKind},
    services::client::{messages::ClientServiceMessage, messages::ClientServiceResponse},
};

use super::types::{GlobalConsensusRef, NetworkManagerRef, RoutingServiceRef};

/// Handles global consensus requests
pub struct GlobalHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Global consensus service
    global_consensus: GlobalConsensusRef<T, G, S>,
    /// Network manager for forwarding
    network_manager: NetworkManagerRef<T, G>,
    /// Routing service for leader discovery
    routing_service: RoutingServiceRef,
}

impl<T, G, S> GlobalHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new global handler
    pub fn new(
        node_id: NodeId,
        global_consensus: GlobalConsensusRef<T, G, S>,
        network_manager: NetworkManagerRef<T, G>,
        routing_service: RoutingServiceRef,
    ) -> Self {
        Self {
            node_id,
            global_consensus,
            network_manager,
            routing_service,
        }
    }

    /// Handle a global consensus request
    pub async fn handle(&self, request: GlobalRequest) -> ConsensusResult<GlobalResponse> {
        let global_guard = self.global_consensus.read().await;
        let service = global_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Global consensus service not available")
        })?;

        // Try to submit locally first
        match service.submit_request(request.clone()).await {
            Ok(response) => {
                // Check if the response is an error variant
                match &response {
                    GlobalResponse::Error { message } => Err(Error::with_context(
                        ErrorKind::InvalidState,
                        message.clone(),
                    )),
                    _ => Ok(response),
                }
            }
            Err(e) if e.is_not_leader() => {
                // We're not the leader, need to forward
                let mut leader = e.get_leader().cloned();

                // If error doesn't have leader, try to get from routing service
                if leader.is_none() {
                    let routing_guard = self.routing_service.read().await;
                    if let Some(routing) = routing_guard.as_ref() {
                        leader = routing.get_global_leader().await;
                    }
                }

                if let Some(leader_id) = leader {
                    if leader_id != self.node_id {
                        // Forward to the leader
                        self.forward_to_leader(request, leader_id).await
                    } else {
                        // We think we're the leader but consensus disagrees
                        Err(e)
                    }
                } else {
                    // No leader known
                    Err(Error::with_context(
                        ErrorKind::InvalidState,
                        "Not the leader and no known leader to forward to",
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Forward a request to the global consensus leader
    async fn forward_to_leader(
        &self,
        request: GlobalRequest,
        leader: NodeId,
    ) -> ConsensusResult<GlobalResponse> {
        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Network manager not available")
        })?;

        let forward_msg = ClientServiceMessage::Global {
            requester_id: self.node_id.clone(),
            request,
        };

        let response = network
            .service_request(
                leader.clone(),
                forward_msg,
                std::time::Duration::from_secs(30),
            )
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Network,
                    format!("Failed to forward request to leader {leader}: {e}"),
                )
            })?;

        match response {
            ClientServiceResponse::Global { response } => Ok(response),
            _ => Err(Error::with_context(
                ErrorKind::Internal,
                "Unexpected response type from leader",
            )),
        }
    }
}

impl<T, G, S> Clone for GlobalHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            global_consensus: self.global_consensus.clone(),
            network_manager: self.network_manager.clone(),
            routing_service: self.routing_service.clone(),
        }
    }
}
