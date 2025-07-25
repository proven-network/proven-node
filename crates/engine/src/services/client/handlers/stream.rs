//! Stream operation handler for client service

use std::sync::Arc;

use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    consensus::group::{GroupRequest, GroupResponse},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{
        events::EventBus,
        routing::{RoutingDecision, RoutingTable},
    },
    services::client::handlers::GroupHandler,
};

/// Handles stream operations by routing them to the appropriate group
pub struct StreamHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Group handler
    group_handler: Arc<GroupHandler<T, G, S>>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
}

impl<T, G, S> StreamHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new stream handler
    pub fn new(
        event_bus: Arc<EventBus>,
        group_handler: Arc<GroupHandler<T, G, S>>,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        Self {
            event_bus,
            group_handler,
            routing_table,
        }
    }

    /// Handle a stream operation request
    pub async fn handle(
        &self,
        stream_name: &str,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // Get stream routing info to determine target group
        let route = self
            .routing_table
            .get_stream_route(stream_name)
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })?;

        match route {
            Some(route) => {
                tracing::debug!(
                    "Stream {} routed to group {:?}",
                    stream_name,
                    route.group_id
                );
                // Delegate to group handler (handles local vs remote)
                self.group_handler.handle(route.group_id, request).await
            }
            None => Err(Error::with_context(
                ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
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
            event_bus: self.event_bus.clone(),
            group_handler: self.group_handler.clone(),
            routing_table: self.routing_table.clone(),
        }
    }
}
