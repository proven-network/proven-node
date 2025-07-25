//! Client API for interacting with the consensus engine
//!
//! This module provides a clean public API for submitting operations
//! and querying the consensus system.

use std::num::NonZero;
use std::pin::Pin;
use std::sync::Arc;

use proven_storage::LogIndex;
use proven_storage::StorageAdaptor;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

use crate::error::Error;
use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse, StreamOperation},
    },
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::client::{ClientService, GroupInfo, StreamInfo},
    services::pubsub::PubSubMessage,
    services::stream::{MessageData, StoredMessage, StreamConfig},
};

/// Client for interacting with the consensus engine
///
/// This provides the main API for:
/// - Stream operations (create, delete, publish)
/// - Group operations (create, delete, query)
/// - Cluster operations (query status)
pub struct Client<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Reference to the client service
    client_service: Arc<ClientService<T, G, S>>,
    /// Node ID for reference
    node_id: NodeId,
}

impl<T, G, S> Client<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new client
    pub(crate) fn new(client_service: Arc<ClientService<T, G, S>>, node_id: NodeId) -> Self {
        Self {
            client_service,
            node_id,
        }
    }

    // Stream Operations

    /// Create a new stream with automatic group assignment
    ///
    /// This method automatically selects an appropriate group for the stream
    /// based on the current node's group membership and load balancing.
    ///
    /// This operation is idempotent - if a stream already exists with the same
    /// name, the operation will succeed silently.
    pub async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
    ) -> ConsensusResult<GlobalResponse> {
        // Get a suitable group for this node
        let group_id = self.client_service.get_suitable_group().await?;

        let stream_name: crate::services::stream::StreamName = name.into();
        let request = GlobalRequest::CreateStream {
            name: stream_name.clone(),
            config,
            group_id,
        };

        match self.client_service.submit_global_request(request).await? {
            GlobalResponse::StreamCreated { name, group_id } => {
                Ok(GlobalResponse::StreamCreated { name, group_id })
            }
            GlobalResponse::StreamAlreadyExists { name, group_id } => {
                // Treat as success for idempotency
                Ok(GlobalResponse::StreamCreated { name, group_id })
            }
            GlobalResponse::Error { message } => Err(Error::with_context(
                crate::error::ErrorKind::OperationFailed,
                message,
            )),
            other => Err(Error::with_context(
                crate::error::ErrorKind::InvalidState,
                format!("Unexpected response: {other:?}"),
            )),
        }
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: String) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::DeleteStream { name: name.into() };
        self.client_service.submit_global_request(request).await
    }

    /// Publish a message to a stream
    pub async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> ConsensusResult<GroupResponse> {
        tracing::debug!(
            "Client::publish called on node {} for stream '{}', payload size: {}",
            self.node_id,
            stream,
            payload.len()
        );

        // First, get stream info to find the group
        let stream_info = self
            .client_service
            .get_stream_info(&stream)
            .await?
            .ok_or_else(|| {
                tracing::debug!("Stream '{}' not found on node {}", stream, self.node_id);
                crate::error::Error::not_found(format!("Stream '{stream}' not found"))
            })?;

        tracing::debug!(
            "Node {}: Stream '{}' is in group {:?}",
            self.node_id,
            stream,
            stream_info.group_id
        );

        // Create message data
        let message = MessageData::with_headers(
            payload,
            metadata
                .map(|m| m.into_iter().collect())
                .unwrap_or_default(),
        );

        // Get current timestamp for the message
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Submit to the group that owns the stream
        let request = GroupRequest::Stream(StreamOperation::Append {
            stream: stream.into(),
            messages: vec![message],
            timestamp,
        });

        tracing::debug!(
            "Node {}: Submitting publish request to group {:?}",
            self.node_id,
            stream_info.group_id
        );

        let result = self
            .client_service
            .submit_group_request(stream_info.group_id, request)
            .await;

        match &result {
            Ok(response) => tracing::debug!(
                "Node {}: Publish succeeded with response: {:?}",
                self.node_id,
                response
            ),
            Err(e) => tracing::debug!("Node {}: Publish failed with error: {}", self.node_id, e),
        }

        result
    }

    /// Get stream information
    pub async fn get_stream_info(&self, name: &str) -> ConsensusResult<Option<StreamInfo>> {
        self.client_service.get_stream_info(name).await
    }

    // Group Operations

    /// Create a new consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::CreateGroup {
            info: crate::foundation::GroupInfo {
                id: group_id,
                members,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                metadata: Default::default(),
            },
        };
        self.client_service.submit_global_request(request).await
    }

    /// Delete a consensus group
    pub async fn delete_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::DissolveGroup { id: group_id };
        self.client_service.submit_global_request(request).await
    }

    /// Get group information
    pub async fn get_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupInfo>> {
        self.client_service.get_group_info(group_id).await
    }

    // Node Operations

    /// Add a node to the cluster
    pub async fn add_node(&self, node_id: NodeId) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::AddNode {
            node_id,
            metadata: Default::default(),
        };
        self.client_service.submit_global_request(request).await
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> ConsensusResult<GlobalResponse> {
        let request = GlobalRequest::RemoveNode { node_id };
        self.client_service.submit_global_request(request).await
    }

    /// Get the node ID of this client
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Read messages from a stream
    pub async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: LogIndex,
        count: LogIndex,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        self.client_service
            .read_stream(&stream_name, start_sequence, count)
            .await
    }

    /// Delete a message from a stream
    pub async fn delete_message(
        &self,
        stream_name: String,
        sequence: LogIndex,
    ) -> ConsensusResult<GroupResponse> {
        // Get stream info to find which group owns it
        let stream_info = self.get_stream_info(&stream_name).await?.ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::NotFound,
                format!("Stream '{stream_name}' not found"),
            )
        })?;

        // Submit delete operation to the group that owns the stream
        let request = GroupRequest::Stream(StreamOperation::Delete {
            stream: stream_name.into(),
            sequence,
        });

        self.client_service
            .submit_group_request(stream_info.group_id, request)
            .await
    }

    /// Stream messages from a stream starting at the given sequence
    ///
    /// Returns a stream that yields messages as they are read. If end_sequence is None,
    /// streams until the last available message.
    pub async fn stream_messages(
        &self,
        stream_name: String,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
    ) -> ConsensusResult<impl Stream<Item = ConsensusResult<StoredMessage>>> {
        // Use the new event bus streaming
        let stream = self
            .client_service
            .stream_messages(&stream_name, start_sequence, end_sequence)
            .await?;

        // Wrap each message in Ok since RecvStream yields items directly
        Ok(stream.map(Ok))
    }

    // PubSub Operations

    /// Publish a message to a PubSub subject
    ///
    /// This uses Core NATS semantics - fire-and-forget with at-most-once delivery.
    /// Messages are ephemeral and not persisted unless a stream subscription is configured.
    pub async fn pubsub_publish(
        &self,
        subject: &str,
        payload: bytes::Bytes,
        headers: Vec<(String, String)>,
    ) -> ConsensusResult<()> {
        self.client_service
            .publish_message(subject, payload, headers)
            .await
    }

    /// Subscribe to messages on a subject pattern
    ///
    /// Returns a subscription ID and a broadcast receiver for messages.
    /// Supports wildcards: * for single token, > for multiple tokens.
    ///
    /// Example patterns:
    /// - "metrics.cpu" - exact match
    /// - "metrics.*" - matches metrics.cpu, metrics.memory, etc.
    /// - "logs.>" - matches logs.app, logs.app.error, etc.
    pub async fn pubsub_subscribe(
        &self,
        subject_pattern: &str,
        queue_group: Option<String>,
    ) -> ConsensusResult<(String, tokio::sync::broadcast::Receiver<PubSubMessage>)> {
        self.client_service
            .subscribe_to_subject(subject_pattern, queue_group)
            .await
    }

    /// Unsubscribe from a PubSub subscription
    pub async fn pubsub_unsubscribe(&self, subscription_id: &str) -> ConsensusResult<()> {
        self.client_service
            .unsubscribe_from_subject(subscription_id)
            .await
    }

    /// Create a stream that yields PubSub messages for a subscription
    ///
    /// This is a convenience method that wraps the broadcast receiver in an async stream.
    pub async fn pubsub_subscribe_stream(
        &self,
        subject_pattern: &str,
        queue_group: Option<String>,
    ) -> ConsensusResult<(String, Pin<Box<dyn Stream<Item = PubSubMessage> + Send>>)> {
        let (sub_id, mut receiver) = self.pubsub_subscribe(subject_pattern, queue_group).await?;

        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(msg) => yield msg,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!("PubSub subscription lagged by {} messages", count);
                        // Continue receiving despite lag
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break;
                    }
                }
            }
        };

        Ok((sub_id, Box::pin(stream)))
    }
}

impl<T, G, S> Clone for Client<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            client_service: self.client_service.clone(),
            node_id: self.node_id.clone(),
        }
    }
}

/// A stream reader for efficiently reading messages from a stream
///
/// This struct provides a Stream interface for reading messages in batches
/// from either local or remote streams.
pub struct StreamReader<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Client service reference
    client_service: Arc<ClientService<T, G, S>>,
    /// Stream name
    stream_name: String,
    /// Current sequence number
    current_sequence: LogIndex,
    /// End sequence (None means stream to end)
    end_sequence: Option<LogIndex>,
    /// Batch size for reading
    batch_size: LogIndex,
    /// Stream session ID (for remote streams)
    session_id: Option<Uuid>,
    /// Whether the stream is local
    is_local: bool,
    /// Buffered messages
    buffer: Vec<StoredMessage>,
    /// Whether we've reached the end
    is_finished: bool,
    /// Whether to follow the stream (wait for new messages)
    follow_mode: bool,
    /// Current backoff delay for polling when no messages available
    poll_backoff: std::time::Duration,
    /// Last time we polled (for backoff calculation)
    last_poll_time: Option<std::time::Instant>,
}

impl<T, G, S> StreamReader<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new stream reader
    async fn new(
        client_service: Arc<ClientService<T, G, S>>,
        stream_name: String,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
    ) -> ConsensusResult<Self> {
        // Check if stream is local or remote
        let _stream_info = client_service
            .get_stream_info(&stream_name)
            .await?
            .ok_or_else(|| {
                Error::with_context(
                    crate::error::ErrorKind::NotFound,
                    format!("Stream '{stream_name}' not found"),
                )
            })?;

        // Determine if stream is local by checking routing
        let is_local = client_service
            .is_stream_local(&stream_name)
            .await
            .unwrap_or(false);

        Ok(Self {
            client_service,
            stream_name,
            current_sequence: start_sequence,
            end_sequence,
            batch_size: LogIndex::new(100).unwrap(), // Default batch size
            session_id: None,
            is_local,
            buffer: Vec::new(),
            is_finished: false,
            follow_mode: false,
            poll_backoff: std::time::Duration::from_millis(100),
            last_poll_time: None,
        })
    }

    /// Set the batch size for reading
    pub fn with_batch_size(mut self, batch_size: LogIndex) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable follow mode to continuously wait for new messages
    pub fn follow(mut self) -> Self {
        self.follow_mode = true;
        self
    }

    /// Read the next batch of messages
    async fn read_next_batch(&mut self) -> ConsensusResult<()> {
        tracing::debug!(
            "read_next_batch: stream={}, current_seq={}, finished={}, follow={}",
            self.stream_name,
            self.current_sequence,
            self.is_finished,
            self.follow_mode
        );

        if self.is_finished {
            return Ok(());
        }

        if self.is_local {
            // Read directly from local storage
            let count = self.batch_size;
            let messages = self
                .client_service
                .read_stream(&self.stream_name, self.current_sequence, count)
                .await?;

            tracing::debug!(
                "read_next_batch: read {} messages from local storage",
                messages.len()
            );

            if messages.is_empty() {
                if !self.follow_mode {
                    self.is_finished = true;
                }
            } else {
                // Check if we've reached the end sequence
                for msg in messages {
                    if let Some(end) = self.end_sequence
                        && msg.sequence >= end
                    {
                        self.is_finished = true;
                        break;
                    }
                    self.buffer.push(msg);
                }
                // Update current_sequence to the next expected sequence
                if let Some(last_msg) = self.buffer.last() {
                    self.current_sequence = last_msg.sequence.saturating_add(1);
                }
            }
        } else {
            // Use streaming protocol for remote streams
            if self.session_id.is_none() {
                // Start a new streaming session
                let (session_id, messages, has_more) = self
                    .client_service
                    .start_streaming_session(
                        &self.stream_name,
                        self.current_sequence,
                        self.end_sequence,
                        self.batch_size,
                    )
                    .await?;

                self.session_id = Some(session_id);
                self.buffer.extend(messages);

                // Update current_sequence to the next expected sequence
                if let Some(last_msg) = self.buffer.last() {
                    self.current_sequence = last_msg.sequence.saturating_add(1);
                }

                if !has_more && !self.follow_mode {
                    self.is_finished = true;
                    // Clean up session
                    if let Some(id) = self.session_id.take() {
                        let _ = self.client_service.cancel_streaming_session(id).await;
                    }
                }
            } else if let Some(session_id) = self.session_id {
                // Continue existing session
                let (messages, has_more) = self
                    .client_service
                    .continue_streaming_session(session_id, self.batch_size)
                    .await?;

                self.buffer.extend(messages);

                // Update current_sequence to the next expected sequence
                if let Some(last_msg) = self.buffer.last() {
                    self.current_sequence = last_msg.sequence.saturating_add(1);
                }

                if !has_more && !self.follow_mode {
                    self.is_finished = true;
                    // Session will be cleaned up automatically
                    self.session_id = None;
                }
            }
        }

        Ok(())
    }
}

impl<T, G, S> Stream for StreamReader<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    type Item = ConsensusResult<StoredMessage>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // If we have buffered messages, return one
        if !self.buffer.is_empty() {
            return std::task::Poll::Ready(Some(Ok(self.buffer.remove(0))));
        }

        // If we're finished and not in follow mode, return None
        if self.is_finished && !self.follow_mode {
            return std::task::Poll::Ready(None);
        }

        // Check if we need to apply backoff in follow mode
        if self.follow_mode
            && self.buffer.is_empty()
            && let Some(last_poll) = self.last_poll_time
        {
            let elapsed = last_poll.elapsed();
            if elapsed < self.poll_backoff {
                // Schedule a wake-up after the backoff period
                let remaining = self.poll_backoff - elapsed;
                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    tokio::time::sleep(remaining).await;
                    waker.wake();
                });
                return std::task::Poll::Pending;
            }
        }

        // We need to use the old approach of creating a new reader to avoid borrow issues
        let client_service = self.client_service.clone();
        let stream_name = self.stream_name.clone();
        let current_sequence = self.current_sequence;
        let end_sequence = self.end_sequence;
        let batch_size = self.batch_size;
        let session_id = self.session_id;
        let is_local = self.is_local;
        let follow_mode = self.follow_mode;
        let poll_backoff = self.poll_backoff;

        // Create a future for reading the next batch
        let fut = async move {
            let mut reader = StreamReader {
                client_service,
                stream_name,
                current_sequence,
                end_sequence,
                batch_size,
                session_id,
                is_local,
                buffer: Vec::new(),
                is_finished: false,
                follow_mode,
                poll_backoff,
                last_poll_time: None,
            };
            reader.read_next_batch().await?;
            Ok::<_, crate::error::Error>(reader)
        };

        // Poll the future
        let mut pinned_fut = Box::pin(fut);
        match pinned_fut.as_mut().poll(cx) {
            std::task::Poll::Ready(Ok(mut reader)) => {
                // Update our state
                self.current_sequence = reader.current_sequence;
                self.session_id = reader.session_id;
                std::mem::swap(&mut self.buffer, &mut reader.buffer);
                self.is_finished = reader.is_finished;
                self.last_poll_time = Some(std::time::Instant::now());

                // Return a message if we have one
                if !self.buffer.is_empty() {
                    // Reset backoff on successful read
                    self.poll_backoff = std::time::Duration::from_millis(100);
                    std::task::Poll::Ready(Some(Ok(self.buffer.remove(0))))
                } else if self.is_finished && !self.follow_mode {
                    std::task::Poll::Ready(None)
                } else if self.follow_mode {
                    // Apply exponential backoff (max 5 seconds)
                    self.poll_backoff =
                        (self.poll_backoff * 2).min(std::time::Duration::from_secs(5));
                    // Wake ourselves up to retry after backoff
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                } else {
                    // Not in follow mode and no messages - we're done
                    std::task::Poll::Ready(None)
                }
            }
            std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Some(Err(e))),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<T, G, S> Drop for StreamReader<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    fn drop(&mut self) {
        // Cancel any active streaming session
        if let Some(session_id) = self.session_id.take() {
            let client_service = self.client_service.clone();
            tokio::spawn(async move {
                let _ = client_service.cancel_streaming_session(session_id).await;
            });
        }
    }
}
