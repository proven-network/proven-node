//! Group consensus handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{events::EventBus, types::ConsensusGroupId},
    services::{client::network::RequestForwarder, group_consensus::commands::SubmitToGroup},
};

/// Handles group consensus requests
pub struct GroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Event bus for sending requests
    event_bus: Arc<EventBus>,
    /// Request forwarder
    forwarder: Arc<RequestForwarder<T, G>>,
    /// Phantom data for S
    _phantom: std::marker::PhantomData<S>,
}

impl<T, G, S> GroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new group handler
    pub fn new(event_bus: Arc<EventBus>, forwarder: Arc<RequestForwarder<T, G>>) -> Self {
        Self {
            event_bus,
            forwarder,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Handle a group consensus request
    pub async fn handle(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Check if group is local using event bus
        use crate::services::routing::commands::IsGroupLocal;

        let is_local = self
            .event_bus
            .request(IsGroupLocal { group_id })
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to check if group is local: {e}"),
                )
            })?;
        tracing::debug!(
            "Group handler checking group {:?}: is_local = {}",
            group_id,
            is_local
        );

        if is_local {
            // Local group - try to submit locally first
            tracing::debug!("Submitting request to local group {:?}", group_id);

            // Use event bus to submit to group
            let submit_request = SubmitToGroup {
                group_id,
                request: request.clone(),
            };

            match self.event_bus.request(submit_request).await {
                Ok(response) => Ok(response),
                Err(event_err) => {
                    // Convert event bus error to consensus error
                    let consensus_err: Error = event_err.into();

                    if consensus_err.is_not_leader() {
                        // We're not the leader for this group
                        // Check if the error contains the leader info
                        if let Some(leader) = consensus_err.get_leader() {
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
                    } else {
                        Err(consensus_err)
                    }
                }
            }
        } else {
            // Remote group - forward to any member
            tracing::debug!("Group {:?} is remote, forwarding request", group_id);
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
            event_bus: self.event_bus.clone(),
            forwarder: self.forwarder.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
