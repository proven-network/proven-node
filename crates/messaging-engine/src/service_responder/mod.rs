//! Service responders handle responses in the engine messaging system.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use tracing::{debug, warn};

use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};
use proven_messaging::stream::InitializedStream;

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;

/// An engine messaging service responder.
#[derive(Clone)]
pub struct EngineMessagingServiceResponder<Tr, G, St, T, D, S, R, RD, RS>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Stream to send responses to (request stream type)
    stream: InitializedEngineStream<Tr, G, St, T, D, S>,
    /// Response stream name where responses should be sent
    response_stream_name: String,
    /// Request ID for correlation
    request_id: String,
    /// Stream sequence number of the request
    stream_sequence: u64,
    /// Phantom data for response type
    _marker: PhantomData<(R, RD, RS)>,
}

impl<Tr, G, St, T, D, S, R, RD, RS> Debug
    for EngineMessagingServiceResponder<Tr, G, St, T, D, S, R, RD, RS>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineMessagingServiceResponder")
            .field("stream", &self.stream)
            .field("response_stream_name", &self.response_stream_name)
            .field("request_id", &self.request_id)
            .field("stream_sequence", &self.stream_sequence)
            .finish()
    }
}

impl<Tr, G, St, T, D, S, R, RD, RS> EngineMessagingServiceResponder<Tr, G, St, T, D, S, R, RD, RS>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Create a new engine messaging service responder.
    #[must_use]
    pub const fn new(
        stream: InitializedEngineStream<Tr, G, St, T, D, S>,
        response_stream_name: String,
        request_id: String,
        stream_sequence: u64,
    ) -> Self {
        Self {
            stream,
            response_stream_name,
            request_id,
            stream_sequence,
            _marker: PhantomData,
        }
    }

    /// Send a response through the engine stream.
    async fn send_response(&self, response: R) -> Result<(), MessagingEngineError> {
        // Convert response to bytes
        let response_bytes: Bytes = response.try_into().map_err(|_| {
            MessagingEngineError::Serialization("Failed to serialize response".to_string())
        })?;

        // Prepare metadata
        let mut metadata = HashMap::new();
        metadata.insert("request_id".to_string(), self.request_id.clone());
        metadata.insert("response_type".to_string(), "service_response".to_string());
        metadata.insert(
            "response_stream".to_string(),
            self.response_stream_name.clone(),
        );

        // Publish to the request stream with metadata indicating it's a response
        // In a real implementation, the response would be routed to the appropriate
        // response stream based on the metadata
        let _seq = self
            .stream
            .publish_with_metadata(response_bytes, metadata)
            .await?;

        debug!(
            "Published response for request '{}' to stream '{}'",
            self.request_id,
            self.stream.name()
        );

        Ok(())
    }
}

/// A used responder for an engine messaging service.
#[derive(Clone, Debug)]
pub struct EngineMessagingUsedResponder;

impl UsedServiceResponder for EngineMessagingUsedResponder {}

#[async_trait]
impl<Tr, G, St, T, TD, TS, R, RD, RS> ServiceResponder<T, TD, TS, R, RD, RS>
    for EngineMessagingServiceResponder<Tr, G, St, T, TD, TS, R, RD, RS>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = TD>
        + TryInto<Bytes, Error = TS>
        + 'static,
    TD: Debug + Send + StdError + Sync + 'static,
    TS: Debug + Send + StdError + Sync + 'static,
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
    type Error = MessagingEngineError;
    type UsedResponder = EngineMessagingUsedResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        debug!("No reply for request '{}'", self.request_id);
        EngineMessagingUsedResponder
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        if let Err(e) = self.send_response(response).await {
            warn!(
                "Failed to send response for request '{}': {:?}",
                self.request_id, e
            );
        }
        EngineMessagingUsedResponder
    }

    async fn stream<W>(self, mut response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // TODO: Implement streaming responses when engine supports it
        // For now, just consume the stream
        use futures::StreamExt;
        while let Some(response) = response_stream.next().await {
            if let Err(e) = self.send_response(response).await {
                warn!(
                    "Failed to send stream response for request '{}': {:?}",
                    self.request_id, e
                );
                break;
            }
        }
        EngineMessagingUsedResponder
    }

    async fn reply_and_delete_request(self, response: R) -> Self::UsedResponder {
        // Send the response first
        if let Err(e) = self.send_response(response).await {
            warn!(
                "Failed to send response for request '{}': {:?}",
                self.request_id, e
            );
        }

        // Delete the request message from the stream
        if let Err(e) = self.stream.delete(self.stream_sequence).await {
            warn!(
                "Failed to delete request message at sequence {} for request '{}': {:?}",
                self.stream_sequence, self.request_id, e
            );
        }

        EngineMessagingUsedResponder
    }

    async fn stream_and_delete_request<W>(self, mut response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // Stream the responses
        use futures::StreamExt;
        while let Some(response) = response_stream.next().await {
            if let Err(e) = self.send_response(response).await {
                warn!(
                    "Failed to send stream response for request '{}': {:?}",
                    self.request_id, e
                );
                break;
            }
        }

        // Delete the request message from the stream
        if let Err(e) = self.stream.delete(self.stream_sequence).await {
            warn!(
                "Failed to delete request message at sequence {} for request '{}': {:?}",
                self.stream_sequence, self.request_id, e
            );
        }

        EngineMessagingUsedResponder
    }

    fn stream_sequence(&self) -> u64 {
        self.stream_sequence
    }
}
