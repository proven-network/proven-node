//! Streaming handlers for the Stream service

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::foundation::events::{Error as EventError, EventMetadata, StreamHandler};
use crate::services::stream::storage::StreamStorageReader;
use crate::services::stream::streaming_commands::StreamMessages;
use crate::services::stream::{StoredMessage, StreamName, StreamService};
use proven_storage::{LogIndex, StorageAdaptor};

/// Handler for streaming messages from a stream
pub struct StreamMessagesHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> StreamMessagesHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait]
impl<S> StreamHandler<StreamMessages> for StreamMessagesHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: StreamMessages,
        _metadata: EventMetadata,
        sink: flume::Sender<StoredMessage>,
    ) -> Result<(), EventError> {
        info!(
            "Creating streaming session for stream: {} (start: {}, end: {:?})",
            request.stream_name, request.start_sequence, request.end_sequence
        );

        // Get the stream using the proper method that handles both in-memory and persistent streams
        let stream_name = StreamName::new(request.stream_name.clone());
        let stream = self
            .stream_service
            .get_stream(&stream_name)
            .await
            .ok_or_else(|| EventError::Internal(format!("Stream '{stream_name}' not found")))?;

        info!(
            "Got stream storage for {}, spawning streaming task",
            request.stream_name
        );

        // Subscribe to stream notifications if we need to tail
        let mut notifier = if request.end_sequence.is_none() {
            self.stream_service.subscribe_to_stream(&stream_name).await
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

                // Try to read a batch of messages
                match stream.read_range(current_sequence, batch_end).await {
                    Ok(messages) => {
                        if messages.is_empty() {
                            // No messages in this range yet
                            // Check if we've read all available messages
                            if let Some((_start, end)) = stream_bounds
                                && current_sequence > end
                            {
                                break;
                            }

                            // If we have a notifier and haven't reached the end, wait for new messages
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
                                // No notifier, just end when empty
                                info!(
                                    "No more messages available, ending stream for {}",
                                    stream_name_str
                                );
                                break;
                            }
                        } else {
                            // Send all messages in the batch
                            for message in messages {
                                // Update current sequence before sending
                                current_sequence = message.sequence.next();

                                if sink.send_async(message).await.is_err() {
                                    // Client disconnected
                                    debug!(
                                        "Client disconnected while streaming from {}",
                                        stream_name_str
                                    );
                                    return;
                                }
                            }
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
