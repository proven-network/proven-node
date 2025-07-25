//! Request forwarder for remote groups

use std::{num::NonZero, sync::Arc};

use proven_storage::LogIndex;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{events::EventBus, types::ConsensusGroupId},
    services::client::{ClientServiceMessage, ClientServiceResponse, types::StreamInfo},
};

use super::super::handlers::types::NetworkManagerRef;

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
    /// Event bus
    event_bus: Arc<EventBus>,
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
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            network_manager,
            event_bus,
        }
    }

    /// Forward a request to a remote group
    pub async fn forward_to_remote_group(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Get routing info using event bus
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        let group_info = routing_info.group_routes.get(&group_id).ok_or_else(|| {
            tracing::error!(
                "Group {:?} not found in routing table. Available groups: {:?}",
                group_id,
                routing_info.group_routes.keys().collect::<Vec<_>>()
            );
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id:?} not found"))
        })?;

        tracing::debug!(
            "Forwarding request for group {:?} - location: {:?}, members: {:?}, leader: {:?}",
            group_id,
            group_info.location,
            group_info.members,
            group_info.leader
        );

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
            .service_request(
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
                        .service_request(
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

    /// Forward a request directly to a known leader
    pub async fn forward_to_leader(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
        leader: NodeId,
    ) -> ConsensusResult<GroupResponse> {
        // Skip if leader is self (shouldn't happen but be defensive)
        if leader == self.node_id {
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

        // Forward the request to the leader
        let message = ClientServiceMessage::Group {
            requester_id: self.node_id.clone(),
            group_id,
            request,
        };

        match network
            .service_request(leader.clone(), message, std::time::Duration::from_secs(30))
            .await
        {
            Ok(ClientServiceResponse::Group { response }) => Ok(response),
            Ok(_) => Err(Error::with_context(
                ErrorKind::Internal,
                "Unexpected response type",
            )),
            Err(e) => Err(Error::with_context(
                ErrorKind::Network,
                format!("Failed to forward request to leader {leader}: {e}"),
            )),
        }
    }

    /// Forward a read request to a group
    pub async fn forward_read_request(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        start_sequence: LogIndex,
        count: LogIndex,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        // Get routing info using event bus
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
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
                .service_request(member.clone(), message, std::time::Duration::from_secs(30))
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

    /// Forward a stream info query to a remote group member
    pub async fn forward_stream_info_query(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<Option<StreamInfo>> {
        // Get routing info to find group members
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        let group_info = routing_info.group_routes.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id:?} not found"))
        })?;

        // Get network manager
        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Network manager not available")
        })?;

        // Try members in order
        let mut last_error = None;
        for member in &group_info.members {
            if member == &self.node_id {
                continue; // Skip self
            }

            // Forward the query request
            let message = ClientServiceMessage::QueryStreamInfo {
                requester_id: self.node_id.clone(),
                stream_name: stream_name.to_string(),
            };

            match network
                .service_request(member.clone(), message, std::time::Duration::from_secs(5))
                .await
            {
                Ok(ClientServiceResponse::StreamInfo { info }) => return Ok(info),
                Ok(_) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Internal,
                        "Unexpected response type",
                    ));
                }
                Err(e) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Network,
                        format!("Failed to query {member}: {e}"),
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

    /// Forward a streaming start request to a remote group
    pub async fn forward_streaming_start(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
        batch_size: LogIndex,
    ) -> ConsensusResult<(
        uuid::Uuid,
        Vec<crate::services::stream::StoredMessage>,
        bool,
    )> {
        // Get routing info using event bus
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        let group_info = routing_info.group_routes.get(&group_id).ok_or_else(|| {
            Error::with_context(ErrorKind::NotFound, format!("Group {group_id:?} not found"))
        })?;

        // Try any member for streaming
        let mut last_error = None;
        for member in &group_info.members {
            if member == &self.node_id {
                continue;
            }

            let network_guard = self.network_manager.read().await;
            let network = network_guard.as_ref().ok_or_else(|| {
                Error::with_context(ErrorKind::Service, "Network manager not available")
            })?;

            let message = ClientServiceMessage::StreamStart {
                requester_id: self.node_id.clone(),
                stream_name: stream_name.to_string(),
                start_sequence: start_sequence.get(),
                end_sequence: end_sequence.map(|e| e.get()),
                batch_size,
            };

            match network
                .service_request(member.clone(), message, std::time::Duration::from_secs(30))
                .await
            {
                Ok(ClientServiceResponse::StreamBatch {
                    session_id,
                    messages,
                    has_more,
                    ..
                }) => return Ok((session_id, messages, has_more)),
                Ok(_) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Internal,
                        "Unexpected response type",
                    ));
                }
                Err(e) => {
                    last_error = Some(Error::with_context(
                        ErrorKind::Network,
                        format!("Failed to start stream from {member}: {e}"),
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

    /// Forward a streaming continue request
    pub async fn forward_streaming_continue(
        &self,
        session_id: uuid::Uuid,
        max_messages: u32,
    ) -> ConsensusResult<(Vec<crate::services::stream::StoredMessage>, bool)> {
        // For continuing a session, we need to send to the same node that started it
        // Since we don't track which node owns which session, we'll need to broadcast
        // In practice, you might want to track session -> node mapping

        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Network manager not available")
        })?;

        // Get all known nodes from topology using event bus
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        // Try all known nodes
        let mut all_nodes = std::collections::HashSet::new();
        for group_info in routing_info.group_routes.values() {
            all_nodes.extend(group_info.members.iter().cloned());
        }

        for node in all_nodes {
            if node == self.node_id {
                continue;
            }

            let message = ClientServiceMessage::StreamContinue {
                requester_id: self.node_id.clone(),
                session_id,
                max_messages,
            };

            match network
                .service_request(node.clone(), message, std::time::Duration::from_secs(30))
                .await
            {
                Ok(ClientServiceResponse::StreamBatch {
                    messages, has_more, ..
                }) => return Ok((messages, has_more)),
                Ok(ClientServiceResponse::StreamError { error, .. }) => {
                    return Err(Error::with_context(ErrorKind::Internal, error));
                }
                Ok(_) => {
                    // Node doesn't have this session, try next
                    continue;
                }
                Err(_) => {
                    // Network error, try next node
                    continue;
                }
            }
        }

        Err(Error::with_context(
            ErrorKind::NotFound,
            format!("Session {session_id} not found on any node"),
        ))
    }

    /// Forward a streaming cancel request
    pub async fn forward_streaming_cancel(&self, session_id: uuid::Uuid) -> ConsensusResult<()> {
        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Network manager not available")
        })?;

        // Get routing info using event bus
        use crate::services::routing::commands::GetRoutingInfo;

        let routing_info = self.event_bus.request(GetRoutingInfo).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        // Try all known nodes
        let mut all_nodes = std::collections::HashSet::new();
        for group_info in routing_info.group_routes.values() {
            all_nodes.extend(group_info.members.iter().cloned());
        }

        for node in all_nodes {
            if node == self.node_id {
                continue;
            }

            let message = ClientServiceMessage::StreamCancel {
                requester_id: self.node_id.clone(),
                session_id,
            };

            match network
                .service_request(node.clone(), message, std::time::Duration::from_secs(30))
                .await
            {
                Ok(ClientServiceResponse::StreamEnd { .. }) => return Ok(()),
                Ok(_) | Err(_) => {
                    // Try next node
                    continue;
                }
            }
        }

        // Even if we couldn't cancel it, don't error - the session will timeout
        Ok(())
    }
}
