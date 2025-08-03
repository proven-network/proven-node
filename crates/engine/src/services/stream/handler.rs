//! Network service handler for Stream service

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};
use proven_storage::{LogIndex, StorageAdaptor, StorageManager};
use std::sync::Arc;
use tokio_stream::StreamExt;

use super::internal::registry::StreamRegistry;
use super::messages::{StreamServiceMessage, StreamServiceResponse};
use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::{Message, RoutingTable, StreamName};

/// Stream service handler
pub struct StreamHandler<S>
where
    S: StorageAdaptor,
{
    /// Stream registry for managing stream storage
    stream_registry: Arc<StreamRegistry<S>>,
    /// Routing table for determining stream locations
    routing_table: Arc<RoutingTable>,
    /// Storage manager for stream access
    storage_manager: Arc<StorageManager<S>>,
}

impl<S> StreamHandler<S>
where
    S: StorageAdaptor,
{
    /// Create a new handler
    pub fn new(
        stream_registry: Arc<StreamRegistry<S>>,
        routing_table: Arc<RoutingTable>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            stream_registry,
            routing_table,
            storage_manager,
        }
    }

    /// Check if a stream can be served locally (we're in the group that owns it)
    async fn can_serve_stream(&self, stream_name: &StreamName) -> ConsensusResult<bool> {
        // Get stream's group from routing table
        if let Ok(Some(stream_route)) = self
            .routing_table
            .get_stream_route(stream_name.as_str())
            .await
        {
            // Check if we're in that group
            match stream_route.placement {
                StreamPlacement::Group(group_id) => {
                    return self
                        .routing_table
                        .is_group_local(group_id)
                        .await
                        .map_err(|e| {
                            Error::with_context(
                                ErrorKind::Internal,
                                format!("Failed to check group location: {e}"),
                            )
                        });
                }
                StreamPlacement::Global => {
                    // Global streams exist on all nodes, so we can always serve them locally
                    return Ok(true);
                }
            }
        }
        // If no routing info, assume we can't serve it
        Ok(false)
    }

    /// Read messages in a range
    async fn read_range(
        &self,
        stream_name: &StreamName,
        start: LogIndex,
        end: LogIndex,
    ) -> ConsensusResult<Vec<Message>> {
        use super::internal::storage::StreamStorageReader;

        // Get the stream storage from registry
        if let Some(stream_storage) = self
            .stream_registry
            .get_or_create_storage(stream_name)
            .await
        {
            // Read the requested range
            return stream_storage
                .read_range(start, end)
                .await
                .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()));
        }

        Err(Error::with_context(
            ErrorKind::NotFound,
            format!("Stream {stream_name} not found"),
        ))
    }

    /// Read messages from a start sequence as a stream
    async fn read_from(
        &self,
        stream_name: &StreamName,
        start: LogIndex,
    ) -> ConsensusResult<impl tokio_stream::Stream<Item = (Message, u64, u64)>> {
        use proven_storage::{LogStorageStreaming, StorageNamespace};

        let stream_name_str = stream_name.to_string();

        // Use proven_storage::LogStorageStreaming directly
        let namespace = StorageNamespace::new(format!("stream_{stream_name_str}"));
        let stream_storage = self.storage_manager.stream_storage();

        let storage_stream =
            LogStorageStreaming::stream_range(&stream_storage, &namespace, Some(start))
                .await
                .map_err(|e| {
                    Error::with_context(ErrorKind::Storage, format!("Failed to create stream: {e}"))
                })?;

        // Convert storage stream to (Message, timestamp, sequence) stream
        let mapped_stream = storage_stream.filter_map(move |result| match result {
            Ok((_seq, bytes)) => match crate::foundation::deserialize_entry(&bytes) {
                Ok((message, timestamp, sequence)) => Some((message, timestamp, sequence)),
                Err(_) => None,
            },
            Err(_) => None,
        });

        Ok(mapped_stream)
    }
}

#[async_trait]
impl<S> Service for StreamHandler<S>
where
    S: StorageAdaptor + Send + Sync + 'static,
{
    type Request = StreamServiceMessage;

    async fn handle(
        &self,
        message: Self::Request,
        _ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        match message {
            StreamServiceMessage::Read {
                stream_name,
                sequence_range,
            } => {
                // Check if we can serve this stream locally
                if let Ok(can_serve) = self.can_serve_stream(&stream_name).await
                    && !can_serve
                {
                    return Ok(StreamServiceResponse::Error(
                        "Cannot serve stream from this node".to_string(),
                    ));
                }

                // Read messages based on range
                let result =
                    if let (Some(start), Some(end)) = (sequence_range.start, sequence_range.end) {
                        self.read_range(&stream_name, start, end).await
                    } else if let Some(start) = sequence_range.start {
                        // Read from start to latest
                        match self.read_from(&stream_name, start).await {
                            Ok(stream) => {
                                let messages: Vec<_> =
                                    futures::StreamExt::map(stream, |(msg, _, _)| msg)
                                        .collect()
                                        .await;
                                Ok(messages)
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        // No range specified - return empty
                        Ok(vec![])
                    };

                match result {
                    Ok(messages) => Ok(StreamServiceResponse::Messages(messages)),
                    Err(e) => Ok(StreamServiceResponse::Error(e.to_string())),
                }
            }
            StreamServiceMessage::Create { .. } => Ok(StreamServiceResponse::Error(
                "Stream creation should go through consensus".to_string(),
            )),
            StreamServiceMessage::Delete { .. } => Ok(StreamServiceResponse::Error(
                "Stream deletion should go through consensus".to_string(),
            )),
            StreamServiceMessage::Query { .. } => Ok(StreamServiceResponse::Error(
                "Streaming queries not implemented".to_string(),
            )),
        }
    }
}
