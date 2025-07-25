//! Core traits for the event system

use async_trait::async_trait;
use bytes::Bytes;
use std::time::Duration;

use super::types::{EventId, EventMetadata, Priority};

/// A fire-and-forget event that can have multiple subscribers
pub trait Event: Clone + Send + Sync + 'static {
    /// Unique type name for this event
    fn event_type() -> &'static str;

    /// Priority for event delivery (default: Normal)
    fn priority() -> Priority {
        Priority::Normal
    }

    /// Whether this event should be persisted for replay
    fn persist() -> bool {
        false
    }

    /// Optional partition key for ordering guarantees
    fn partition_key(&self) -> Option<&str> {
        None
    }

    /// Convert to bytes for zero-copy scenarios
    fn to_bytes(&self) -> Option<Bytes> {
        None
    }
}

/// A request that expects a single response
pub trait Request: Send + Sync + 'static {
    /// The response type for this request
    type Response: Send + Sync + 'static;

    /// Unique type name for this request
    fn request_type() -> &'static str;

    /// Default timeout for this request type
    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }

    /// Priority for request processing
    fn priority() -> Priority {
        Priority::Normal
    }
}

/// A request that expects a stream of responses
pub trait StreamRequest: Send + Sync + 'static {
    /// The response type for each item in the stream
    type Item: Send + Sync + 'static;

    /// Unique type name for this request
    fn request_type() -> &'static str;

    /// Default timeout for the entire stream
    fn default_timeout() -> Duration {
        Duration::from_secs(300) // 5 minutes default
    }
}

/// Handler for events (fire-and-forget)
#[async_trait]
pub trait EventHandler<E: Event>: Send + Sync + 'static {
    /// Handle an event
    async fn handle(&self, event: E, metadata: EventMetadata);

    /// Called when the handler is being shut down
    async fn shutdown(&self) {}
}

/// Handler for requests (request-response)
#[async_trait]
pub trait RequestHandler<R: Request>: Send + Sync + 'static {
    /// Handle a request and return a response
    async fn handle(
        &self,
        request: R,
        metadata: EventMetadata,
    ) -> Result<R::Response, crate::foundation::events::Error>;

    /// Called when the handler is being shut down
    async fn shutdown(&self) {}
}

/// Handler for stream requests
#[async_trait]
pub trait StreamHandler<R: StreamRequest>: Send + Sync + 'static {
    /// Handle a stream request and return a stream of responses
    async fn handle(
        &self,
        request: R,
        metadata: EventMetadata,
        sink: flume::Sender<R::Item>,
    ) -> Result<(), crate::foundation::events::Error>;

    /// Called when the handler is being shut down
    async fn shutdown(&self) {}
}
