//! Query handler for client service

use std::num::NonZero;

use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::client::types::{GroupInfo, StreamInfo},
};

use super::types::{GlobalConsensusRef, GroupConsensusRef, RoutingServiceRef};
use crate::services::client::network::RequestForwarder;
use std::sync::Arc;

/// Handles query requests (GetStreamInfo, GetGroupInfo)
pub struct QueryHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Group consensus service
    group_consensus: GroupConsensusRef<T, G, S>,
    /// Global consensus service
    global_consensus: GlobalConsensusRef<T, G, S>,
    /// Routing service
    routing_service: RoutingServiceRef,
    /// Request forwarder
    forwarder: Arc<RequestForwarder<T, G>>,
}

impl<T, G, S> QueryHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new query handler
    pub fn new(
        group_consensus: GroupConsensusRef<T, G, S>,
        global_consensus: GlobalConsensusRef<T, G, S>,
        routing_service: RoutingServiceRef,
        forwarder: Arc<RequestForwarder<T, G>>,
    ) -> Self {
        Self {
            group_consensus,
            global_consensus,
            routing_service,
            forwarder,
        }
    }

    /// Get stream information
    pub async fn get_stream_info(&self, stream_name: &str) -> ConsensusResult<Option<StreamInfo>> {
        // Get routing service for stream info
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Routing service not available")
        })?;

        // Check if stream exists in routing table
        match routing.get_stream_routing_info(stream_name).await {
            Ok(Some(route)) => {
                // Check if the group is local or remote
                let is_local = routing.is_group_local(route.group_id).await?;

                if is_local {
                    // Query the local group consensus for stream state
                    let group_guard = self.group_consensus.read().await;
                    let group_service = group_guard.as_ref().ok_or_else(|| {
                        Error::with_context(
                            ErrorKind::Service,
                            "Group consensus service not available",
                        )
                    })?;

                    // Get the stream state from the group
                    match group_service
                        .get_stream_state(route.group_id, stream_name)
                        .await
                    {
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
                            last_sequence: NonZero::new(stream_state.next_sequence.get().saturating_sub(1)).unwrap_or(stream_state.next_sequence),
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
                            last_sequence: NonZero::new(1).unwrap(), // No messages yet, start at 1
                            message_count: 0, // No messages yet
                        };
                            Ok(Some(stream_info))
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    // For remote groups, forward the query to a node in that group
                    self.forwarder
                        .forward_stream_info_query(route.group_id, stream_name)
                        .await
                }
            }
            Ok(None) => {
                // Stream not found
                Ok(None)
            }
            Err(e) => Err(Error::with_context(
                ErrorKind::Service,
                format!("Failed to query routing info: {e}"),
            )),
        }
    }

    /// Get group information
    pub async fn get_group_info(
        &self,
        _group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupInfo>> {
        // Get group info from global consensus
        let global_guard = self.global_consensus.read().await;
        let _global_service = global_guard.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Service, "Global consensus service not available")
        })?;

        // TODO: Get group info from global consensus
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
            group_consensus: self.group_consensus.clone(),
            global_consensus: self.global_consensus.clone(),
            routing_service: self.routing_service.clone(),
            forwarder: self.forwarder.clone(),
        }
    }
}
