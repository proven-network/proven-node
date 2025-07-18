//! Service responders handle responses in the engine messaging system.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use tracing::{debug, warn};

use proven_messaging::service_responder::{ServiceResponder, UsedServiceResponder};

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;

/// An engine messaging service responder.
#[derive(Clone, Debug)]
pub struct EngineMessagingServiceResponder<R, RD, RS>
where
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
    /// Stream to send responses to
    stream: InitializedEngineStream<R, RD, RS>,
    /// Response stream name where responses should be sent
    response_stream_name: String,
    /// Request ID for correlation
    request_id: String,
}

impl<R, RD, RS> EngineMessagingServiceResponder<R, RD, RS>
where
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
    pub const fn new<T, D, S>(
        stream: &InitializedEngineStream<T, D, S>,
        response_stream_name: String,
        request_id: String,
    ) -> Self
    where
        T: Clone
            + Debug
            + Send
            + Sync
            + TryFrom<Bytes, Error = D>
            + TryInto<Bytes, Error = S>
            + 'static,
        D: Debug + Send + StdError + Sync + 'static,
        S: Debug + Send + StdError + Sync + 'static,
    {
        // Note: In a real implementation, we would need to handle the type conversion
        // between the request stream type and response stream type
        Self {
            stream: unsafe { std::mem::transmute_copy(&stream) },
            response_stream_name,
            request_id,
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

        // Publish to response stream
        let _seq = self
            .stream
            .publish_with_metadata(response_bytes, metadata)
            .await?;

        debug!(
            "Published response for request '{}' to stream '{}'",
            self.request_id, self.response_stream_name
        );

        Ok(())
    }
}

/// A used responder for an engine messaging service.
#[derive(Clone, Debug)]
pub struct EngineMessagingUsedResponder;

impl UsedServiceResponder for EngineMessagingUsedResponder {}

#[async_trait]
impl<T, TD, TS, R, RD, RS> ServiceResponder<T, TD, TS, R, RD, RS>
    for EngineMessagingServiceResponder<R, RD, RS>
where
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
        // Engine doesn't support deletion, just reply
        if let Err(e) = self.send_response(response).await {
            warn!(
                "Failed to send response for request '{}': {:?}",
                self.request_id, e
            );
        }
        EngineMessagingUsedResponder
    }

    async fn stream_and_delete_request<W>(self, mut response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin,
    {
        // Engine doesn't support deletion, just stream
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

    fn stream_sequence(&self) -> u64 {
        // TODO: Implement when engine supports it
        0
    }
}
