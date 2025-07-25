//! Query handler for client service

use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{events::EventBus, routing::RoutingTable, types::ConsensusGroupId},
    services::client::types::{GroupInfo, StreamInfo},
};

use crate::services::client::network::RequestForwarder;
use std::sync::Arc;

/// Handles query requests (GetStreamInfo, GetGroupInfo)
pub struct QueryHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Event bus for queries
    event_bus: Arc<EventBus>,
    /// Request forwarder
    forwarder: Arc<RequestForwarder<T, G>>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
    /// Phantom data for S
    _phantom: std::marker::PhantomData<S>,
}

impl<T, G, S> QueryHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new query handler
    pub fn new(
        event_bus: Arc<EventBus>,
        forwarder: Arc<RequestForwarder<T, G>>,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        Self {
            event_bus,
            forwarder,
            routing_table,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get stream information
    pub async fn get_stream_info(&self, stream_name: &str) -> ConsensusResult<Option<StreamInfo>> {
        // Get stream routing info directly from routing table
        match self
            .routing_table
            .get_stream_route(stream_name)
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })? {
            Some(route) => {
                // For now, return basic info from routing table
                // TODO: Implement GetStreamState handler in group consensus
                let stream_info = StreamInfo {
                    name: stream_name.to_string(),
                    config: Default::default(), // Use default config for now
                    group_id: route.group_id,
                    last_sequence: LogIndex::new(1).unwrap(), // Default to 1
                    message_count: 0,                         // Unknown for now
                };
                Ok(Some(stream_info))
            }
            None => {
                // Stream not found
                Ok(None)
            }
        }
    }

    /// Get group information
    pub async fn get_group_info(
        &self,
        _group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupInfo>> {
        // TODO: Get group info from global consensus using event bus
        // For now, return not implemented
        Err(Error::with_context(
            ErrorKind::Internal,
            "Group info query not yet implemented",
        ))
    }
}

impl<T, G, S> Clone for QueryHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            event_bus: self.event_bus.clone(),
            forwarder: self.forwarder.clone(),
            routing_table: self.routing_table.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}
