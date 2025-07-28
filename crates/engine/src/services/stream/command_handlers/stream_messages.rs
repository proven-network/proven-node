//! Handler for StreamMessages command (streaming)

use async_trait::async_trait;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::foundation::events::{Error as EventError, EventMetadata, StreamHandler};
use crate::foundation::{Message, RoutingTable, StreamName};
use crate::services::stream::commands::StreamMessages;
use crate::services::stream::internal::registry::StreamRegistry;
use crate::services::stream::internal::storage::StreamStorageReader;
use proven_storage::{LogStorageStreaming, StorageNamespace};
use tokio_stream::StreamExt;

/// Handler for streaming messages from a stream
pub struct StreamMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Stream registry for managing stream storage
    stream_registry: Arc<StreamRegistry<S>>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
    /// Network manager for remote requests
    network_manager: Arc<NetworkManager<T, G, A>>,
}

impl<T, G, A, S> StreamMessagesHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(
        node_id: NodeId,
        stream_registry: Arc<StreamRegistry<S>>,
        routing_table: Arc<RoutingTable>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> Self {
        Self {
            node_id,
            stream_registry,
            routing_table,
            network_manager,
        }
    }

    async fn can_serve_stream(
        &self,
        stream_name: &StreamName,
    ) -> Result<bool, crate::error::Error> {
        use crate::error::{Error, ErrorKind};

        // Get stream's group from routing table
        if let Some(stream_route) = self
            .routing_table
            .get_stream_route(stream_name.as_str())
            .await?
        {
            // Check if we're in that group
            return self
                .routing_table
                .is_group_local(stream_route.group_id)
                .await
                .map_err(|e| {
                    Error::with_context(
                        ErrorKind::Internal,
                        format!("Failed to check group location: {e}"),
                    )
                });
        }
        // If no routing info, assume we can't serve it
        Ok(false)
    }

    async fn handle_remote_stream(
        &self,
        request: StreamMessages,
        sink: flume::Sender<(Message, u64, u64)>,
    ) -> Result<(), EventError> {
        use std::collections::HashMap;

        let stream_name = StreamName::new(request.stream_name.clone());

        // Get stream's route from routing table
        let stream_route = self
            .routing_table
            .get_stream_route(stream_name.as_str())
            .await
            .map_err(|e| EventError::Internal(format!("Failed to get stream route: {e}")))?
            .ok_or_else(|| {
                EventError::Internal(format!("Stream route not found for {stream_name}"))
            })?;

        // Get the nodes in the group
        let group_route = self
            .routing_table
            .get_group_route(stream_route.group_id)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to get group route: {e}")))?
            .ok_or_else(|| {
                EventError::Internal(format!(
                    "Group route not found for group {}",
                    stream_route.group_id
                ))
            })?;

        let nodes = group_route.members;

        // Pick a node to stream from (preferably not ourselves)
        let target_node = nodes
            .iter()
            .find(|&n| n != &self.node_id)
            .or_else(|| nodes.first()) // Fallback to any node if all are us
            .ok_or_else(|| EventError::Internal("No nodes available for streaming".to_string()))?
            .clone();

        if target_node == self.node_id {
            return Err(EventError::Internal(
                "Cannot proxy to self, but stream is not local".to_string(),
            ));
        }

        info!(
            "Proxying stream {} to remote node {}",
            request.stream_name, target_node
        );

        // Prepare metadata for the stream
        let mut metadata = HashMap::new();
        metadata.insert("stream_name".to_string(), request.stream_name.clone());
        metadata.insert(
            "start_sequence".to_string(),
            request.start_sequence.to_string(),
        );
        if let Some(end) = request.end_sequence {
            metadata.insert("end_sequence".to_string(), end.to_string());
        }

        // Open stream to remote node
        let remote_stream = self
            .network_manager
            .open_stream(target_node.clone(), "stream_messages", metadata)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to open remote stream: {e}")))?;

        // Spawn a task to handle the remote streaming
        let stream_name = request.stream_name.clone();
        let target_node_clone = target_node.clone();
        tokio::spawn(async move {
            // Forward messages from remote stream to local sink
            while let Some(data) = remote_stream.recv().await {
                match ciborium::from_reader::<(Message, u64, u64), _>(data.as_ref()) {
                    Ok(entry) => {
                        if sink.send_async(entry).await.is_err() {
                            debug!(
                                "Client disconnected while streaming from remote {}",
                                stream_name
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize remote message: {}", e);
                        // Continue with next message rather than failing completely
                    }
                }
            }

            info!(
                "Remote streaming completed for {} from node {}",
                stream_name, target_node_clone
            );
        });

        // Return immediately, allowing the handler to complete
        Ok(())
    }
}

#[async_trait]
impl<T, G, A, S> StreamHandler<StreamMessages> for StreamMessagesHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: StreamMessages,
        _metadata: EventMetadata,
        sink: flume::Sender<(Message, u64, u64)>,
    ) -> Result<(), EventError> {
        info!(
            "Creating streaming session for stream: {} (start: {}, end: {:?})",
            request.stream_name, request.start_sequence, request.end_sequence
        );

        let stream_name = StreamName::new(request.stream_name.clone());

        // Check if we can serve this stream locally
        match self.can_serve_stream(&stream_name).await {
            Ok(true) => {
                // Local stream - proceed with streaming
            }
            Ok(false) => {
                // Remote stream - proxy via network
                return self.handle_remote_stream(request, sink).await;
            }
            Err(e) => {
                return Err(EventError::Internal(format!(
                    "Failed to check stream location: {e}"
                )));
            }
        }

        // Check if stream exists
        if !self.stream_registry.stream_exists(&stream_name) {
            return Err(EventError::Internal(format!(
                "Stream '{stream_name}' not found"
            )));
        }

        info!(
            "Starting streaming for {} from sequence {} to {:?}",
            request.stream_name, request.start_sequence, request.end_sequence
        );

        // Get the storage manager and create namespace
        let storage_manager = self.stream_registry.storage_manager.clone();
        let namespace = StorageNamespace::new(format!("stream_{}", request.stream_name));

        // Use the storage's streaming API directly
        let storage = storage_manager.stream_storage();
        let mut storage_stream = LogStorageStreaming::stream_range(
            &storage,
            &namespace,
            request.start_sequence,
            request.end_sequence,
        )
        .await
        .map_err(|e| EventError::Internal(format!("Failed to create stream: {e}")))?;

        // Spawn a task to handle the streaming
        let stream_name = request.stream_name.clone();
        tokio::spawn(async move {
            // Stream messages to the sink
            while let Some(result) = storage_stream.next().await {
                match result {
                    Ok((_index, bytes)) => {
                        // Deserialize the entry
                        match crate::foundation::deserialize_entry(&bytes) {
                            Ok((message, timestamp, sequence)) => {
                                if sink
                                    .send_async((message, timestamp, sequence))
                                    .await
                                    .is_err()
                                {
                                    // Client disconnected
                                    debug!(
                                        "Client disconnected while streaming from {}",
                                        stream_name
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to deserialize message from stream {}: {}",
                                    stream_name, e
                                );
                                // Continue with next message
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error reading from stream {}: {}", stream_name, e);
                        // On storage error, stop streaming
                        break;
                    }
                }
            }

            info!("Streaming completed for {}", stream_name);
        });

        // Return immediately, allowing the handler to complete
        Ok(())
    }
}
