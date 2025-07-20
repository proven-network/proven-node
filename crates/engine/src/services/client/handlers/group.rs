//! Group consensus handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::client::network::RequestForwarder,
};

use super::types::{GroupConsensusRef, RoutingServiceRef};

/// Handles group consensus requests
pub struct GroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Group consensus service
    group_consensus: GroupConsensusRef<T, G, S>,
    /// Routing service
    routing_service: RoutingServiceRef,
    /// Request forwarder
    forwarder: Arc<RequestForwarder<T, G>>,
}

impl<T, G, S> GroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new group handler
    pub fn new(
        group_consensus: GroupConsensusRef<T, G, S>,
        routing_service: RoutingServiceRef,
        forwarder: Arc<RequestForwarder<T, G>>,
    ) -> Self {
        Self {
            group_consensus,
            routing_service,
            forwarder,
        }
    }

    /// Handle a group consensus request
    pub async fn handle(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Check if group is local
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Routing service not available")
        })?;

        if routing.is_group_local(group_id).await? {
            // Local group - try to submit locally first
            let group_guard = self.group_consensus.read().await;
            let service = group_guard.as_ref().ok_or_else(|| {
                Error::with_context(ErrorKind::Service, "Group consensus service not available")
            })?;

            match service.submit_to_group(group_id, request.clone()).await {
                Ok(response) => Ok(response),
                Err(e) if e.is_not_leader() => {
                    // We're not the leader for this group
                    // Check if the error contains the leader info
                    if let Some(leader) = e.get_leader() {
                        // Forward directly to the known leader
                        self.forwarder
                            .forward_to_leader(group_id, request, leader.clone())
                            .await
                    } else {
                        // No leader known, try forwarding to find one
                        self.forwarder
                            .forward_to_remote_group(group_id, request)
                            .await
                    }
                }
                Err(e) => Err(e),
            }
        } else {
            // Remote group - forward to any member
            self.forwarder
                .forward_to_remote_group(group_id, request)
                .await
        }
    }
}

impl<T, G, S> Clone for GroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            group_consensus: self.group_consensus.clone(),
            routing_service: self.routing_service.clone(),
            forwarder: self.forwarder.clone(),
        }
    }
}
