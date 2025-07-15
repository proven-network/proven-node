//! Type-safe stream consumer
//!
//! This module provides the `Consumer<T>` type for consuming messages
//! from streams in a type-safe manner.

use super::{
    errors::{ClientError, ClientResult},
    stream::Message,
    types::MessageType,
};
use crate::{
    core::{engine::Engine, group::GroupStreamOperation},
    operations::handlers::{
        GroupStreamOperationResponse, group_responses::StreamOperationResponse,
    },
};

use std::{marker::PhantomData, sync::Arc};

use proven_governance::Governance;
use proven_network::Transport;
use proven_storage::LogStorage;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Configuration for a stream consumer
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Starting sequence number (0 means start from beginning)
    pub start_sequence: u64,
    /// Maximum number of messages to buffer
    pub buffer_size: usize,
    /// Whether to follow the stream (wait for new messages)
    pub follow: bool,
    /// Batch size for fetching messages
    pub batch_size: u32,
    /// Name for this consumer (for debugging)
    pub name: Option<String>,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            start_sequence: 0,
            buffer_size: 100,
            follow: true,
            batch_size: 10,
            name: None,
        }
    }
}

/// A type-safe consumer for reading messages from a stream
///
/// The consumer provides an async iterator interface for reading
/// messages from a stream. It handles batching and buffering
/// transparently.
///
/// # Examples
///
/// ```ignore
/// let mut consumer = stream.consumer(ConsumerConfig::default()).await?;
///
/// while let Some(result) = consumer.next().await {
///     match result {
///         Ok(msg) => println!("Got message: {:?}", msg.data),
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// ```
pub struct Consumer<M, T, G, L>
where
    M: MessageType,
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Stream name
    stream_name: String,
    /// Configuration
    config: ConsumerConfig,
    /// Channel for receiving messages
    receiver: mpsc::Receiver<ClientResult<Message<M>>>,
    /// Background task handle
    _task_handle: JoinHandle<()>,
    /// Phantom data for the message type and generics
    _phantom: PhantomData<(M, T, G, L)>,
}

impl<M, T, G, L> Consumer<M, T, G, L>
where
    M: MessageType,
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Create a new consumer
    pub(crate) async fn new(
        stream_name: String,
        config: ConsumerConfig,
        engine: Arc<Engine<T, G, L>>,
    ) -> ClientResult<Self> {
        // Verify stream exists
        engine
            .get_stream_info(&stream_name)
            .await
            .map_err(ClientError::Consensus)?
            .ok_or_else(|| ClientError::StreamNotFound(stream_name.clone()))?;

        // Create channel for messages
        let (sender, receiver) = mpsc::channel(config.buffer_size);

        // Spawn background task to fetch messages
        let task_handle = tokio::spawn(Self::fetch_loop(
            stream_name.clone(),
            config.clone(),
            engine,
            sender,
        ));

        Ok(Self {
            stream_name,
            config,
            receiver,
            _task_handle: task_handle,
            _phantom: PhantomData,
        })
    }

    /// Background task that fetches messages from the stream
    async fn fetch_loop(
        stream_name: String,
        config: ConsumerConfig,
        engine: Arc<Engine<T, G, L>>,
        sender: mpsc::Sender<ClientResult<Message<M>>>,
    ) {
        let mut current_sequence = config.start_sequence;
        let mut consecutive_empty = 0;

        loop {
            // Check if receiver is closed
            if sender.is_closed() {
                break;
            }

            // Fetch a batch of messages
            let mut found_messages = false;

            for seq_offset in 0..config.batch_size {
                let sequence = current_sequence + seq_offset as u64;

                // Try to get the message
                let operation =
                    GroupStreamOperation::direct_get_from_stream(stream_name.clone(), sequence);

                match engine.route_stream_operation(&stream_name, operation).await {
                    Ok(response) if response.is_success() => {
                        match response {
                            GroupStreamOperationResponse::Stream(
                                StreamOperationResponse::Read {
                                    sequence: seq,
                                    data,
                                    metadata,
                                    ..
                                },
                            ) => {
                                // Convert to typed message
                                match M::try_from(data.clone()) {
                                    Ok(typed_data) => {
                                        let message = Message {
                                            sequence: seq,
                                            timestamp: chrono::Utc::now().timestamp_millis() as u64,
                                            data: typed_data,
                                            metadata: metadata.unwrap_or_default(),
                                        };

                                        if sender.send(Ok(message)).await.is_err() {
                                            // Receiver closed
                                            return;
                                        }

                                        found_messages = true;
                                        current_sequence = sequence + 1;
                                    }
                                    Err(e) => {
                                        let err = ClientError::deserialization(e);
                                        if sender.send(Err(err)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Not a read response, skip
                                break;
                            }
                        }
                    }
                    Ok(_) => {
                        // Message doesn't exist yet
                        break;
                    }
                    Err(e) => {
                        let err = ClientError::Consensus(e);
                        if sender.send(Err(err)).await.is_err() {
                            return;
                        }

                        // Wait a bit before retrying
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        break;
                    }
                }
            }

            if found_messages {
                consecutive_empty = 0;
            } else {
                consecutive_empty += 1;

                if !config.follow {
                    // Not following, so we're done
                    break;
                }

                // Exponential backoff when no messages found
                let wait_ms = std::cmp::min(100 * (1 << consecutive_empty), 5000);
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
            }
        }
    }

    /// Get the next message from the consumer
    ///
    /// Returns `None` when the stream ends (if not following) or
    /// if the consumer is dropped.
    pub async fn next(&mut self) -> Option<ClientResult<Message<M>>> {
        self.receiver.recv().await
    }

    /// Try to get the next message without blocking
    pub fn try_next(&mut self) -> Option<ClientResult<Message<M>>> {
        self.receiver.try_recv().ok()
    }

    /// Get the stream name
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Get the consumer configuration
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }
}

/// Builder for creating consumers with custom configuration
pub struct ConsumerBuilder {
    config: ConsumerConfig,
}

impl ConsumerBuilder {
    /// Create a new consumer builder
    pub fn new() -> Self {
        Self {
            config: ConsumerConfig::default(),
        }
    }

    /// Set the starting sequence number
    pub fn start_sequence(mut self, sequence: u64) -> Self {
        self.config.start_sequence = sequence;
        self
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set whether to follow the stream
    pub fn follow(mut self, follow: bool) -> Self {
        self.config.follow = follow;
        self
    }

    /// Set the batch size
    pub fn batch_size(mut self, size: u32) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the consumer name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = Some(name.into());
        self
    }

    /// Build the consumer configuration
    pub fn build(self) -> ConsumerConfig {
        self.config
    }
}

impl Default for ConsumerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
