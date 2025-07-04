//! Subscription responders handle subscription responses.

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

use proven_messaging::subscription_responder::{SubscriptionResponder, UsedSubscriptionResponder};

/// Maximum batch size for streaming responses.
const MAX_BATCH_SIZE: usize = 1024 * 1024; // 1MB

/// A used responder for a consensus subscription.
#[derive(Clone, Debug)]
pub struct ConsensusUsedSubscriptionResponder;

impl UsedSubscriptionResponder for ConsensusUsedSubscriptionResponder {}

/// A responder for a consensus subscription.
#[derive(Clone, Debug)]
pub struct ConsensusSubscriptionResponder<G, A, R, RD, RS>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
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
    /// Subject name this responder belongs to
    subject_name: String,
    /// Request ID for correlation
    request_id: String,
    /// Type marker
    _marker: PhantomData<(G, A, R, RD, RS)>,
}

impl<G, A, R, RD, RS> ConsensusSubscriptionResponder<G, A, R, RD, RS>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
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
    /// Creates a new consensus subscription responder.
    #[must_use]
    pub const fn new(subject_name: String, request_id: String) -> Self {
        Self {
            subject_name,
            request_id,
            _marker: PhantomData,
        }
    }

    /// Get the subject name for this responder.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn subject_name(&self) -> &str {
        &self.subject_name
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
        // Prepare response metadata for consensus publishing
        let mut response_metadata = HashMap::new();
        response_metadata.insert("request_id".to_string(), self.request_id.clone());
        response_metadata.insert("subject_name".to_string(), self.subject_name.clone());
        response_metadata.insert(
            "response_type".to_string(),
            "subscription_response".to_string(),
        );

        if let Some(meta) = metadata {
            response_metadata.insert("stream_metadata".to_string(), meta);
        }

        // For now, simulate response publishing to consensus system
        // In a full implementation, this would publish to the subject's response stream
        // that subscribers can receive responses from
        debug!(
            "Publishing subscription response for subject '{}' request '{}': {} bytes (metadata: {:?})",
            self.subject_name,
            self.request_id,
            response_data.len(),
            response_metadata
        );

        // Note: The actual publishing would look like:
        // self.consensus_subject.publish_with_metadata(response_data.clone(), response_metadata).await?;
    }
}

#[async_trait]
impl<G, A, R, RD, RS> SubscriptionResponder<R, RD, RS>
    for ConsensusSubscriptionResponder<G, A, R, RD, RS>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
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
    type UsedResponder = ConsensusUsedSubscriptionResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        debug!("No reply for subscription request '{}'", self.request_id);
        ConsensusUsedSubscriptionResponder
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        // Convert response to bytes
        match response.try_into() as Result<Bytes, RS> {
            Ok(response_bytes) => {
                self.send_response(&response_bytes, None);
            }
            Err(e) => {
                warn!(
                    "Failed to serialize subscription response for request '{}': {:?}",
                    self.request_id, e
                );
            }
        }

        ConsensusUsedSubscriptionResponder
    }

    async fn stream<W>(self, response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin,
    {
        let batched_stream = Self::batch_stream(response_stream);
        let mut peekable_stream = batched_stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);
        let mut stream_id = 0;

        debug!(
            "Starting stream response for subscription request '{}'",
            self.request_id
        );

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
            "Completed stream response for subscription request '{}' ({} batches)",
            self.request_id, stream_id
        );
        ConsensusUsedSubscriptionResponder
    }
}
