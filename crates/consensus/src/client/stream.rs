//! Type-safe stream operations
//!
//! This module provides the `Stream<T>` type which offers a strongly-typed
//! interface for working with consensus streams.

use super::{
    consumer::{Consumer, ConsumerConfig},
    errors::{ClientError, ClientResult},
    types::MessageType,
};
use crate::{
    ConsensusGroupId,
    config::StreamConfig,
    core::{engine::Engine, group::GroupStreamOperation},
    operations::{
        GlobalOperation, StreamManagementOperation,
        handlers::{GroupStreamOperationResponse, group_responses::StreamOperationResponse},
    },
};

use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use proven_governance::Governance;
use proven_network::Transport;
use proven_storage::LogStorage;

/// A type-safe handle to a consensus stream
///
/// This struct provides methods for publishing and reading typed messages
/// from a specific stream. The type parameter `T` must implement `MessageType`.
///
/// # Examples
///
/// ```ignore
/// // Get a stream handle
/// let stream: Stream<MyMessage> = client.get_stream("orders").await?;
///
/// // Publish a message
/// let sequence = stream.publish(MyMessage { id: 123 }).await?;
///
/// // Read a specific message
/// if let Some(msg) = stream.get(sequence).await? {
///     println!("Message: {:?}", msg.data);
/// }
/// ```
pub struct Stream<M, T, G, L>
where
    M: MessageType,
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Stream name
    name: String,
    /// Stream configuration
    config: StreamConfig,
    /// Reference to the consensus engine
    engine: Arc<Engine<T, G, L>>,
    /// Phantom data for the message type
    _marker: PhantomData<M>,
}

impl<M, T, G, L> Stream<M, T, G, L>
where
    M: MessageType,
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Create a new stream handle
    pub(crate) fn new(name: String, config: StreamConfig, engine: Arc<Engine<T, G, L>>) -> Self {
        Self {
            name,
            config,
            engine,
            _marker: PhantomData,
        }
    }

    /// Get the stream name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the stream configuration
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    /// Publish a typed message to the stream
    ///
    /// Returns the sequence number of the published message.
    pub async fn publish(&self, message: M) -> ClientResult<u64> {
        // Convert message to bytes
        let bytes: Bytes = message
            .try_into()
            .map_err(|e: M::SerializeError| ClientError::serialization(e))?;

        // Use the existing stream operation
        let operation = GroupStreamOperation::publish_to_stream(
            self.name.clone(),
            bytes,
            None, // metadata
        );

        let response = self
            .engine
            .route_stream_operation(&self.name, operation)
            .await
            .map_err(ClientError::Consensus)?;

        if response.is_success() {
            Ok(response.sequence().unwrap_or(0))
        } else {
            Err(ClientError::Internal(
                response
                    .error()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Failed to publish".to_string()),
            ))
        }
    }

    /// Publish a typed message with metadata
    pub async fn publish_with_metadata(
        &self,
        message: M,
        metadata: HashMap<String, String>,
    ) -> ClientResult<u64> {
        // Convert message to bytes
        let bytes: Bytes = message
            .try_into()
            .map_err(|e: M::SerializeError| ClientError::serialization(e))?;

        let operation =
            GroupStreamOperation::publish_to_stream(self.name.clone(), bytes, Some(metadata));

        let response = self
            .engine
            .route_stream_operation(&self.name, operation)
            .await
            .map_err(ClientError::Consensus)?;

        if response.is_success() {
            Ok(response.sequence().unwrap_or(0))
        } else {
            Err(ClientError::Internal(
                response
                    .error()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Failed to publish".to_string()),
            ))
        }
    }

    /// Get a specific message by sequence number
    ///
    /// Returns `None` if the message doesn't exist or has been deleted.
    pub async fn get(&self, sequence: u64) -> ClientResult<Option<Message<M>>> {
        let operation = GroupStreamOperation::direct_get_from_stream(self.name.clone(), sequence);

        let response = self
            .engine
            .route_stream_operation(&self.name, operation)
            .await
            .map_err(ClientError::Consensus)?;

        if !response.is_success() {
            return Ok(None);
        }

        // Extract data from response
        match response {
            GroupStreamOperationResponse::Stream(StreamOperationResponse::Read {
                sequence,
                data,
                metadata,
                ..
            }) => {
                // Convert bytes to typed message
                let message = M::try_from(data.clone())
                    .map_err(|e: M::DeserializeError| ClientError::deserialization(e))?;

                Ok(Some(Message {
                    sequence,
                    timestamp: chrono::Utc::now().timestamp_millis() as u64, // TODO: Add timestamp to response
                    data: message,
                    metadata: metadata.unwrap_or_default(),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Get the last message in the stream
    pub async fn get_last(&self) -> ClientResult<Option<Message<M>>> {
        // First get the last sequence number
        let last_seq = self.last_sequence().await?;

        if last_seq == 0 {
            return Ok(None);
        }

        self.get(last_seq).await
    }

    /// Get the last sequence number in the stream
    pub async fn last_sequence(&self) -> ClientResult<u64> {
        // TODO: This needs a proper implementation once StreamManager exposes
        // a public method to get the last sequence or stream metadata.
        // For now, we'll return 0 as a placeholder.

        // Verify the stream exists
        let _stream_info = self
            .engine
            .get_stream_info(&self.name)
            .await
            .map_err(ClientError::Consensus)?
            .ok_or_else(|| ClientError::StreamNotFound(self.name.clone()))?;

        // Return 0 as placeholder - this should be implemented properly
        // when the stream manager API is updated
        Ok(0)
    }

    /// Create a consumer for this stream
    pub async fn consumer(&self, config: ConsumerConfig) -> ClientResult<Consumer<M, T, G, L>> {
        Consumer::new(self.name.clone(), config, self.engine.clone()).await
    }

    /// Get stream information
    pub async fn info(&self) -> ClientResult<StreamInfo> {
        let stream_info = self
            .engine
            .get_stream_info(&self.name)
            .await
            .map_err(ClientError::Consensus)?
            .ok_or_else(|| ClientError::StreamNotFound(self.name.clone()))?;

        Ok(StreamInfo {
            name: self.name.clone(),
            config: stream_info.config,
            group_id: stream_info.group_id,
            created_at: 0, // TODO: Add these fields to core StreamInfo
            last_updated: 0,
            message_count: 0,
            bytes_stored: 0,
        })
    }

    /// Delete this stream
    ///
    /// This consumes the stream handle to prevent further operations.
    pub async fn delete(self) -> ClientResult<()> {
        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Delete {
            name: self.name.clone(),
        });

        let response = self
            .engine
            .submit_global_request(operation)
            .await
            .map_err(ClientError::Consensus)?;

        if response.is_success() {
            Ok(())
        } else {
            Err(ClientError::Internal(
                response
                    .error()
                    .unwrap_or("Failed to delete stream")
                    .to_string(),
            ))
        }
    }
}

/// Information about a stream
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream name
    pub name: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Assigned consensus group
    pub group_id: ConsensusGroupId,
    /// When the stream was created
    pub created_at: u64,
    /// Last update timestamp
    pub last_updated: u64,
    /// Number of messages in the stream
    pub message_count: u64,
    /// Total bytes stored
    pub bytes_stored: u64,
}

/// A typed message from a stream
#[derive(Debug, Clone)]
pub struct Message<T> {
    /// Sequence number
    pub sequence: u64,
    /// Timestamp when the message was stored
    pub timestamp: u64,
    /// The actual message data
    pub data: T,
    /// Message metadata
    pub metadata: HashMap<String, String>,
}

impl<T> Message<T> {
    /// Create a new message (mainly for testing)
    pub fn new(sequence: u64, timestamp: u64, data: T) -> Self {
        Self {
            sequence,
            timestamp,
            data,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the message
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

// Clone implementation for Stream
impl<M, T, G, L> Clone for Stream<M, T, G, L>
where
    M: MessageType + Send + Sync + 'static,
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            config: self.config.clone(),
            engine: self.engine.clone(),
            _marker: PhantomData,
        }
    }
}
