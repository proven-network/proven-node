//! Streaming implementation for Stream service
//!
//! This module implements the streaming protocol for efficient stream message delivery.
//! It handles incoming stream requests from remote nodes and proxies them to local storage.

use async_trait::async_trait;
use bytes::Bytes;
use proven_network::{NetworkResult, Stream, StreamingService};
use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::foundation::StreamName;
use crate::services::stream::internal::registry::StreamRegistry;

/// Stream messages streaming service for handling remote stream requests
pub struct StreamMessagesStreamingService<S>
where
    S: StorageAdaptor,
{
    stream_registry: Arc<StreamRegistry<S>>,
}

impl<S> StreamMessagesStreamingService<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_registry: Arc<StreamRegistry<S>>) -> Self {
        Self { stream_registry }
    }
}

#[async_trait]
impl<S> StreamingService for StreamMessagesStreamingService<S>
where
    S: StorageAdaptor + 'static,
{
    fn stream_type(&self) -> &'static str {
        "stream_messages"
    }

    async fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        metadata: HashMap<String, String>,
    ) -> NetworkResult<()> {
        info!(
            "Handling stream_messages stream from {} with metadata: {:?}",
            peer, metadata
        );

        // Parse metadata
        let stream_name = metadata
            .get("stream_name")
            .ok_or_else(|| {
                proven_network::NetworkError::InvalidMessage("Missing stream_name".to_string())
            })?
            .clone();

        // If no start_sequence provided, start from beginning
        let start_sequence = metadata
            .get("start_sequence")
            .and_then(|s| s.parse::<u64>().ok())
            .and_then(LogIndex::new);

        debug!(
            "Creating local stream for {} from {:?}",
            stream_name, start_sequence
        );

        // Check if stream exists
        let stream_name_obj = StreamName::new(stream_name.clone());
        if !self.stream_registry.stream_exists(&stream_name_obj) {
            return Err(proven_network::NetworkError::InvalidMessage(format!(
                "Stream '{stream_name}' not found"
            )));
        }

        // Get the storage manager and create namespace
        let namespace = proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));
        let storage = self.stream_registry.storage_manager.stream_storage();

        // Use the storage's streaming API directly
        let mut storage_stream = match proven_storage::LogStorageStreaming::stream_range(
            &storage,
            &namespace,
            start_sequence,
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                return Err(proven_network::NetworkError::InvalidMessage(format!(
                    "Failed to create stream: {e}"
                )));
            }
        };

        // Stream messages to the remote peer
        while let Some(result) = storage_stream.next().await {
            match result {
                Ok((_index, bytes)) => {
                    // Deserialize the entry
                    match crate::foundation::deserialize_entry(&bytes) {
                        Ok((message, timestamp, sequence)) => {
                            // Serialize the tuple
                            let mut data = Vec::new();
                            match ciborium::ser::into_writer(
                                &(message, timestamp, sequence),
                                &mut data,
                            ) {
                                Ok(_) => {
                                    if stream.send(Bytes::from(data)).await.is_err() {
                                        debug!("Remote peer {} disconnected", peer);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to serialize message: {}", e);
                                    // Continue with next message
                                }
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

        info!("Stream to {} completed", peer);
        Ok(())
    }
}
