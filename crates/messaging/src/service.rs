use crate::service_handler::ServiceHandler;
use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for service errors
pub trait ServiceError: Error + Send + Sync + 'static {}

/// Marker trait for service options
pub trait ServiceOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream which can handle requests.
#[async_trait]
pub trait Service
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the service.
    type Error: ServiceError;

    /// The options for the service.
    type Options: ServiceOptions;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync;

    /// The response type for the service.
    type ResponseType: Clone + Debug + Send + Sync;

    /// The stream type for the service.
    type StreamType: Stream;

    /// Creates a new service.
    async fn new<X>(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ServiceHandler<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// Gets the last sequence number processed by the service.
    async fn last_seq(&self) -> Result<u64, Self::Error>;

    /// Gets the stream for the service.
    fn stream(&self) -> Self::StreamType;
}
