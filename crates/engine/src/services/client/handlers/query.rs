//! Query handler for client service

use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{events::EventBus, types::ConsensusGroupId},
    services::{
        client::types::{GroupInfo, StreamInfo},
        group_consensus::commands::GetStreamState,
    },
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
    pub fn new(event_bus: Arc<EventBus>, forwarder: Arc<RequestForwarder<T, G>>) -> Self {
        Self {
            event_bus,
            forwarder,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get stream information
    pub async fn get_stream_info(&self, stream_name: &str) -> ConsensusResult<Option<StreamInfo>> {
        // Get stream routing info
        use crate::services::routing::commands::{GetStreamRoutingInfo, IsGroupLocal};

        match self
            .event_bus
            .request(GetStreamRoutingInfo {
                stream_name: stream_name.to_string(),
            })
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })? {
            Some(route) => {
                // Check if the group is local or remote
                let is_local = self
                    .event_bus
                    .request(IsGroupLocal {
                        group_id: route.group_id,
                    })
                    .await
                    .map_err(|e| {
                        Error::with_context(
                            ErrorKind::Service,
                            format!("Failed to check if group is local: {e}"),
                        )
                    })?;

                if is_local {
                    // Query the local group consensus for stream state using event bus
                    let get_stream_state = GetStreamState {
                        group_id: route.group_id,
                        stream_name: crate::services::stream::StreamName::new(stream_name),
                    };

                    // Get the stream state from the group
                    match self.event_bus.request(get_stream_state).await {
                        Ok(Some(stream_state)) => {
                            // Stream exists with state info
                            let stream_info = StreamInfo {
                            name: stream_name.to_string(),
                            config: route.config.unwrap_or({
                                // Fallback config if not stored in route
                                crate::services::stream::StreamConfig {
                                    max_message_size: 1024 * 1024,
                                    retention: crate::services::stream::config::RetentionPolicy::Forever,
                                    persistence_type: crate::services::stream::config::PersistenceType::Persistent,
                                    allow_auto_create: false,
                                }
                            }),
                            group_id: route.group_id,
                            last_sequence: LogIndex::new(stream_state.next_sequence.get().saturating_sub(1)).unwrap_or(stream_state.next_sequence),
                            message_count: stream_state.stats.message_count,
                        };
                            Ok(Some(stream_info))
                        }
                        Ok(None) => {
                            // Stream exists in routing but has no state yet (no messages)
                            // Return info based on routing alone
                            let stream_info = StreamInfo {
                            name: stream_name.to_string(),
                            config: route.config.unwrap_or({
                                // Fallback config if not stored in route
                                crate::services::stream::StreamConfig {
                                    max_message_size: 1024 * 1024,
                                    retention: crate::services::stream::config::RetentionPolicy::Forever,
                                    persistence_type: crate::services::stream::config::PersistenceType::Persistent,
                                    allow_auto_create: false,
                                }
                            }),
                            group_id: route.group_id,
                            last_sequence: LogIndex::new(1).unwrap(), // No messages yet, start at 1
                            message_count: 0, // No messages yet
                        };
                            Ok(Some(stream_info))
                        }
                        Err(e) => {
                            let consensus_err: Error = e.into();
                            Err(consensus_err)
                        }
                    }
                } else {
                    // For remote groups, forward the query to a node in that group
                    self.forwarder
                        .forward_stream_info_query(route.group_id, stream_name)
                        .await
                }
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
            _phantom: std::marker::PhantomData,
        }
    }
}
