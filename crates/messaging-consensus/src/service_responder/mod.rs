//! Service responders handle responses in the consensus network.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::Stream;
use tracing::{debug, warn};

use proven_attestation::Attestor;
use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};
use proven_messaging::stream::InitializedStream;
use proven_topology::TopologyAdaptor;

/// Maximum batch size for streaming responses.
const MAX_BATCH_SIZE: usize = 1024 * 1024; // 1MB

/// A consensus service responder.
#[derive(Clone, Debug)]
pub struct ConsensusServiceResponder<G, A, T, D, S, R, RD, RS>
where
    G: proven_topology::TopologyAdaptor + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Whether the service has caught up with the stream
    caught_up: bool,
    /// Sequence number of the request being handled
    request_sequence: u64,
    /// Request ID for correlation
    request_id: String,
    /// Response stream name where responses should be sent
    response_stream_name: String,
    /// Request stream for deletion operations
    request_stream: crate::stream::InitializedConsensusStream<G, A, T, D, S>,
    /// Type markers
    _marker: PhantomData<(T, D, S, R, RD, RS)>,
}

impl<G, A, T, D, S, R, RD, RS> ConsensusServiceResponder<G, A, T, D, S, R, RD, RS>
where
    G: proven_topology::TopologyAdaptor + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Create a new consensus service responder.
    #[must_use]
    pub const fn new(
        caught_up: bool,
        request_sequence: u64,
        request_id: String,
        response_stream_name: String,
        request_stream: crate::stream::InitializedConsensusStream<G, A, T, D, S>,
    ) -> Self {
        Self {
            caught_up,
            request_sequence,
            request_id,
            response_stream_name,
            request_stream,
            _marker: PhantomData,
        }
    }

    /// Batch multiple responses into a single message for efficient streaming.
    fn batch_stream<W>(stream: W) -> impl Stream<Item = Bytes> + Send + Unpin
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let mut stream = Box::pin(stream);
        let mut current_batch = BytesMut::with_capacity(MAX_BATCH_SIZE);
        let mut current_size = 0;

        futures::stream::poll_fn(move |cx| {
            loop {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        if let Ok(item_bytes) = item.try_into() as Result<Bytes, RS> {
                            let item_size = item_bytes.len();
                            let total_size = item_size + 4; // 4 bytes for length prefix

                            if !current_batch.is_empty()
                                && current_size + total_size > MAX_BATCH_SIZE
                            {
                                // Yield current batch
                                let batch = current_batch.split().freeze();
                                current_batch.reserve(MAX_BATCH_SIZE);

                                // Start new batch with current item
                                current_batch
                                    .put_u32(u32::try_from(item_bytes.len()).unwrap_or(u32::MAX));
                                current_batch.extend_from_slice(&item_bytes);
                                current_size = total_size;

                                return Poll::Ready(Some(batch));
                            }

                            // Add to current batch
                            current_batch
                                .put_u32(u32::try_from(item_bytes.len()).unwrap_or(u32::MAX));
                            current_batch.extend_from_slice(&item_bytes);
                            current_size += total_size;
                        }
                    }
                    Poll::Ready(None) => {
                        return if current_batch.is_empty() {
                            Poll::Ready(None)
                        } else {
                            let batch = current_batch.split().freeze();
                            Poll::Ready(Some(batch))
                        };
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
    }

    /// Send a response through the consensus system.
    fn send_response(&self, response_data: &Bytes, metadata: Option<String>) {
        if !self.caught_up {
            debug!(
                "Service not caught up, skipping response for request {}",
                self.request_id
            );
            return;
        }

        // Prepare response metadata for consensus publishing
        let mut response_metadata = HashMap::new();
        response_metadata.insert("request_id".to_string(), self.request_id.clone());
        response_metadata.insert("response_type".to_string(), "service_response".to_string());

        if let Some(meta) = metadata {
            response_metadata.insert("stream_metadata".to_string(), meta);
        }

        // For now, simulate response publishing to consensus system
        // In a full implementation, this would publish to the actual response stream
        // that the client subscribes to for receiving responses
        debug!(
            "Publishing response for request {} to stream {}: {} bytes (metadata: {:?})",
            self.request_id,
            self.response_stream_name,
            response_data.len(),
            response_metadata
        );

        // Note: The actual publishing would look like:
        // self.consensus_stream.publish_with_metadata(response_data.clone(), response_metadata).await?;
    }
}

#[async_trait]
impl<G, A, T, D, S, R, RD, RS> ServiceResponder<T, D, S, R, RD, RS>
    for ConsensusServiceResponder<G, A, T, D, S, R, RD, RS>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    type Error = crate::error::MessagingConsensusError;
    type UsedResponder = ConsensusUsedResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        debug!("No reply for request {}", self.request_id);
        ConsensusUsedResponder {}
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        // Convert response to bytes
        match response.try_into() as Result<Bytes, RS> {
            Ok(response_bytes) => {
                self.send_response(&response_bytes, None);
            }
            Err(e) => {
                warn!(
                    "Failed to serialize response for request {}: {:?}",
                    self.request_id, e
                );
            }
        }

        ConsensusUsedResponder {}
    }

    async fn reply_and_delete_request(self, response: R) -> Self::UsedResponder {
        // Store values before moving self
        let request_sequence = self.request_sequence;
        let request_id = self.request_id.clone();
        let request_stream = self.request_stream.clone();

        // Send the reply first
        let result = self.reply(response).await;

        // Delete the original request from the consensus stream
        if let Err(e) = request_stream.delete(request_sequence).await {
            warn!(
                "Failed to delete request {} at sequence {}: {:?}",
                request_id, request_sequence, e
            );
        } else {
            debug!(
                "Successfully deleted request {} at sequence {} from consensus stream",
                request_id, request_sequence
            );
        }

        result
    }

    async fn stream<W>(self, response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        let batched_stream = Self::batch_stream(response_stream);
        let mut peekable_stream = batched_stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);
        let mut stream_id = 0;

        debug!("Starting stream response for request {}", self.request_id);

        loop {
            match (
                pinned_stream.as_mut().next().await,
                pinned_stream.as_mut().peek().await,
            ) {
                (None, _) => {
                    // End of stream - send empty marker
                    self.send_response(
                        &Bytes::new(),
                        Some(format!("stream-end:{}:empty", self.request_id)),
                    );
                    break;
                }
                (Some(batched_bytes), Some(_)) => {
                    // More items coming
                    stream_id += 1;
                    self.send_response(
                        &batched_bytes,
                        Some(format!("stream:{}:{}", self.request_id, stream_id)),
                    );
                }
                (Some(batched_bytes), None) => {
                    // Last item
                    stream_id += 1;
                    self.send_response(
                        &batched_bytes,
                        Some(format!("stream-end:{}:{}", self.request_id, stream_id)),
                    );
                    break;
                }
            }
        }

        debug!(
            "Completed stream response for request {} ({} batches)",
            self.request_id, stream_id
        );
        ConsensusUsedResponder {}
    }

    async fn stream_and_delete_request<W>(self, response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // Store values before moving self
        let request_sequence = self.request_sequence;
        let request_id = self.request_id.clone();
        let request_stream = self.request_stream.clone();

        // Send the stream first
        let result = self.stream(response_stream).await;

        // Delete the original request from the consensus stream
        if let Err(e) = request_stream.delete(request_sequence).await {
            warn!(
                "Failed to delete request {} at sequence {}: {:?}",
                request_id, request_sequence, e
            );
        } else {
            debug!(
                "Successfully deleted request {} at sequence {} from consensus stream",
                request_id, request_sequence
            );
        }

        result
    }

    fn stream_sequence(&self) -> u64 {
        self.request_sequence
    }
}

/// Used responder for consensus services.
#[derive(Debug)]
pub struct ConsensusUsedResponder;

impl UsedServiceResponder for ConsensusUsedResponder {}
