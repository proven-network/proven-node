//! Stream operation handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    services::{client::handlers::GroupHandler, routing::RouteDecision},
};

use super::types::RoutingServiceRef;

/// Handles stream operations by routing them to the appropriate group
pub struct StreamHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Routing service
    routing_service: RoutingServiceRef,
    /// Group handler
    group_handler: Arc<GroupHandler<T, G, S>>,
}

impl<T, G, S> StreamHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new stream handler
    pub fn new(
        routing_service: RoutingServiceRef,
        group_handler: Arc<GroupHandler<T, G, S>>,
    ) -> Self {
        Self {
            routing_service,
            group_handler,
        }
    }

    /// Handle a stream operation request
    pub async fn handle(
        &self,
        stream_name: &str,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Get routing service to determine target group
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Routing service not available")
        })?;

        // Route the stream operation
        match routing.route_stream_operation(stream_name, vec![]).await {
            Ok(RouteDecision::RouteToGroup(group_id)) => {
                // Delegate to group handler (handles local vs remote)
                self.group_handler.handle(group_id, request).await
            }
            Ok(RouteDecision::Reject(reason)) => {
                Err(Error::with_context(ErrorKind::Validation, reason))
            }
            Ok(decision) => Err(Error::with_context(
                ErrorKind::Internal,
                format!("Unexpected routing decision: {decision:?}"),
            )),
            Err(e) => Err(Error::with_context(
                ErrorKind::Service,
                format!("Routing failed: {e}"),
            )),
        }
    }
}

impl<T, G, S> Clone for StreamHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            routing_service: self.routing_service.clone(),
            group_handler: self.group_handler.clone(),
        }
    }
}
