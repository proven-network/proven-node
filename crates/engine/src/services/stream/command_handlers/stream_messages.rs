//! Handler for StreamMessages command (streaming)

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{Error as EventError, EventMetadata, StreamHandler};
use crate::foundation::{Message, RoutingTable, StreamName};
use crate::services::stream::commands::StreamMessages;
use crate::services::stream::internal::storage::{StreamStorageImpl, StreamStorageReader};
use dashmap::DashMap;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tokio::sync::watch;

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
    /// Stream storage instances
    streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
    /// Stream notifiers
    stream_notifiers: Arc<DashMap<StreamName, watch::Sender<LogIndex>>>,
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
        streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
        stream_notifiers: Arc<DashMap<StreamName, watch::Sender<LogIndex>>>,
        routing_table: Arc<RoutingTable>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> Self {
        Self {
            node_id,
            streams,
            stream_notifiers,
            routing_table,
            network_manager,
        }
    }

    async fn get_stream(
        &self,
        stream_name: &StreamName,
    ) -> Option<Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>> {
        self.streams.get(stream_name).map(|entry| entry.clone())
    }

    async fn subscribe_to_stream(
        &self,
        stream_name: &StreamName,
    ) -> Option<watch::Receiver<LogIndex>> {
        self.stream_notifiers
            .get(stream_name)
            .map(|notifier| notifier.subscribe())
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
                // Remote stream - we need to implement streaming proxy
                return Err(EventError::Internal(
                    "Remote stream streaming not yet implemented".to_string(),
                ));
            }
            Err(e) => {
                return Err(EventError::Internal(format!(
                    "Failed to check stream location: {e}"
                )));
            }
        }

        // Get the stream storage
        let stream = self
            .get_stream(&stream_name)
            .await
            .ok_or_else(|| EventError::Internal(format!("Stream '{stream_name}' not found")))?;

        info!(
            "Got stream storage for {}, spawning streaming task",
            request.stream_name
        );

        // Subscribe to stream notifications if we need to tail
        let mut notifier = if request.end_sequence.is_none() {
            self.subscribe_to_stream(&stream_name).await
        } else {
            None
        };

        // Get the stream bounds to know when we've reached the end
        let stream_bounds = stream.bounds().await.ok().flatten();

        // Spawn a task to stream messages
        let stream_name_str = request.stream_name.clone();
        tokio::spawn(async move {
            let mut current_sequence = request.start_sequence;

            loop {
                // Check if we've reached the end boundary
                if let Some(end_seq) = request.end_sequence
                    && current_sequence >= end_seq
                {
                    info!(
                        "Reached end sequence {} for stream {}",
                        end_seq, stream_name_str
                    );
                    break;
                }

                // Read messages in small batches for efficiency
                let batch_size = 100u64;
                let batch_end = if let Some(end_seq) = request.end_sequence {
                    std::cmp::min(current_sequence.saturating_add(batch_size), end_seq)
                } else {
                    current_sequence.saturating_add(batch_size)
                };

                // Try to read a batch of messages with metadata
                match stream
                    .read_range_with_metadata(current_sequence, batch_end)
                    .await
                {
                    Ok(messages) => {
                        if messages.is_empty() {
                            // No messages in this range yet
                            // If we have a notifier (follow mode), wait for new messages
                            if let Some(ref mut notify_rx) = notifier {
                                tokio::select! {
                                    // Wait for new messages
                                    Ok(_) = notify_rx.changed() => {
                                        // New messages available, continue the loop
                                        debug!("Notified of new messages in stream {}", stream_name_str);
                                        continue;
                                    }
                                    // Check if client disconnected periodically
                                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                                        if sink.is_disconnected() {
                                            info!("Client disconnected while streaming {}", stream_name_str);
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            } else {
                                // No notifier (bounded query)
                                // Check if we've read all available messages
                                if let Some((_start, end)) = stream_bounds
                                    && current_sequence > end
                                {
                                    info!(
                                        "Reached end of available messages for bounded query on stream {}",
                                        stream_name_str
                                    );
                                    break;
                                }
                                // For bounded queries, if we're still within bounds but no messages,
                                // wait a bit and continue
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                continue;
                            }
                        } else {
                            // Send all messages in the batch
                            let message_count = messages.len() as u64;
                            for (message, timestamp, sequence) in messages {
                                if sink
                                    .send_async((message, timestamp, sequence))
                                    .await
                                    .is_err()
                                {
                                    // Client disconnected
                                    debug!(
                                        "Client disconnected while streaming from {}",
                                        stream_name_str
                                    );
                                    return;
                                }
                            }
                            // Update current sequence after sending all messages
                            current_sequence = current_sequence.saturating_add(message_count);
                        }
                    }
                    Err(e) => {
                        error!(
                            "Error reading from stream {} at sequence {}: {}",
                            stream_name_str, current_sequence, e
                        );
                        // On error, we'll stop streaming
                        break;
                    }
                }
            }

            debug!("Streaming task for {} completed", stream_name_str);
        });

        // Give the task a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        info!(
            "Streaming session created successfully for stream {}",
            request.stream_name
        );

        Ok(())
    }
}
