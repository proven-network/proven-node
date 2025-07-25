//! Global consensus handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::{
    consensus::global::{GlobalRequest, GlobalResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::events::EventBus,
    services::{
        client::{
            events::ClientServiceEvent, messages::ClientServiceMessage,
            messages::ClientServiceResponse,
        },
        global_consensus::commands::SubmitGlobalRequest,
    },
};

use super::types::NetworkManagerRef;

/// Handles global consensus requests
pub struct GlobalHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Network manager for forwarding
    network_manager: NetworkManagerRef<T, G>,
    /// Event bus for publishing events
    event_bus: Arc<EventBus>,
    /// Phantom data for S
    _phantom: std::marker::PhantomData<S>,
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
        network_manager: NetworkManagerRef<T, G>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            network_manager,
            event_bus,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a global consensus request
    pub async fn handle(&self, request: GlobalRequest) -> ConsensusResult<GlobalResponse> {
        // Use event bus to submit to global consensus
        let submit_request = SubmitGlobalRequest {
            request: request.clone(),
        };

        // Try to submit locally first
        match self.event_bus.request(submit_request).await {
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
            Err(event_err) => {
                // Convert event bus error to consensus error
                let consensus_err: Error = event_err.into();

                if consensus_err.is_not_leader() {
                    // We're not the leader, need to forward
                    let leader = consensus_err.get_leader().cloned();

                    if let Some(leader_id) = leader {
                        if leader_id != self.node_id {
                            // Forward to the leader
                            self.forward_to_leader(request, leader_id).await
                        } else {
                            // We think we're the leader but consensus disagrees
                            Err(consensus_err)
                        }
                    } else {
                        // No leader known
                        Err(Error::with_context(
                            ErrorKind::InvalidState,
                            "Not the leader and no known leader to forward to",
                        ))
                    }
                } else {
                    Err(consensus_err)
                }
            }
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
            ClientServiceResponse::Global { response } => {
                // Check if we learned about an existing stream
                if let GlobalResponse::StreamAlreadyExists { name, group_id } = &response {
                    // Publish event so routing service can learn about this stream
                    let event = ClientServiceEvent::LearnedStreamExists {
                        stream_name: name.clone(),
                        group_id: *group_id,
                    };
                    self.event_bus.emit(event);
                }
                Ok(response)
            }
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
            network_manager: self.network_manager.clone(),
            event_bus: self.event_bus.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
