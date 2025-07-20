//! Request forwarder for remote groups

use std::sync::Arc;

use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::client::{ClientServiceMessage, ClientServiceResponse},
};

use super::super::handlers::types::{NetworkManagerRef, RoutingServiceRef};

/// Forwards requests to remote groups
pub struct RequestForwarder<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: NetworkManagerRef<T, G>,
    /// Routing service
    routing_service: RoutingServiceRef,
}

impl<T, G> RequestForwarder<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    /// Create a new request forwarder
    pub fn new(
        node_id: NodeId,
        network_manager: NetworkManagerRef<T, G>,
        routing_service: RoutingServiceRef,
    ) -> Self {
        Self {
            node_id,
            network_manager,
            routing_service,
        }
    }

    /// Forward a request to a remote group
    pub async fn forward_to_remote_group(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Get routing service to find the best node
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Routing service not available")
        })?;

        // Get group info to find a member
        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        let group_info = routing_info.group_routes.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id:?} not found"))
        })?;

        // Find a responsive member (prefer leader if known)
        let target_node = if let Some(leader) = &group_info.leader {
            leader.clone()
        } else if !group_info.members.is_empty() {
            // Pick any member
            group_info.members.first().unwrap().clone()
        } else {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                format!("Group {group_id:?} has no members"),
            ));
        };

        // Skip if target is self
        if target_node == self.node_id {
            return Err(Error::with_context(
                ErrorKind::Internal,
                "Cannot forward to self",
            ));
        }

        // Get network manager
        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Network manager not available")
        })?;

        // Forward the request
        let message = ClientServiceMessage::Group {
            requester_id: self.node_id.clone(),
            group_id,
            request: request.clone(),
        };

        match network
            .request_service(
                target_node.clone(),
                message,
                std::time::Duration::from_secs(30),
            )
            .await
        {
            Ok(ClientServiceResponse::Group { response }) => Ok(response),
            Ok(_) => Err(Error::with_context(
                ErrorKind::Internal,
                "Unexpected response type",
            )),
            Err(e) => {
                // Try other members if the first one fails
                for member in &group_info.members {
                    if member == &target_node || member == &self.node_id {
                        continue;
                    }

                    let message = ClientServiceMessage::Group {
                        requester_id: self.node_id.clone(),
                        group_id,
                        request: request.clone(),
                    };

                    match network
                        .request_service(
                            member.clone(),
                            message,
                            std::time::Duration::from_secs(30),
                        )
                        .await
                    {
                        Ok(ClientServiceResponse::Group { response }) => return Ok(response),
                        Ok(_) => continue,
                        Err(_) => continue,
                    }
                }

                Err(Error::with_context(
                    ErrorKind::Network,
                    format!("Failed to forward request: {e}"),
                ))
            }
        }
    }

    /// Forward a read request to a group
    pub async fn forward_read_request(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        // Get routing service to find the best node
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Routing service not available")
        })?;

        // Get group info to find a member
        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        let group_info = routing_info.group_routes.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id:?} not found"))
        })?;

        // Try any member for reads (no need for leader)
        let mut last_error = None;
        for member in &group_info.members {
            if member == &self.node_id {
                continue; // Skip self
            }

            // Get network manager
            let network_guard = self.network_manager.read().await;
            let network = network_guard.as_ref().ok_or_else(|| {
                Error::with_context(ErrorKind::Service, "Network manager not available")
            })?;

            // Forward the read request
            let message = ClientServiceMessage::StreamRead {
                requester_id: self.node_id.clone(),
                stream_name: stream_name.to_string(),
                start_sequence,
                count,
            };

            match network
                .request_service(member.clone(), message, std::time::Duration::from_secs(30))
                .await
            {
                Ok(ClientServiceResponse::StreamRead { messages }) => return Ok(messages),
                Ok(_) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Internal,
                        "Unexpected response type",
                    ));
                }
                Err(e) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Network,
                        format!("Failed to read from {member}: {e}"),
                    ));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            Error::with_context(
                ErrorKind::InvalidState,
                format!("No available members in group {group_id:?}"),
            )
        }))
    }
}
