use crate::service_handler::ServiceHandler;
use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for client errors
pub trait ClientError: Error + Send + Sync + 'static {}

/// Marker trait for client options
pub trait ClientOptions: Clone + Send + Sync + 'static {}

/// A trait representing a client of a service the sends requests.
#[async_trait]
pub trait Client
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the client.
    type Error: ClientError;

    /// The options for the service.
    type Options: ClientOptions;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync + 'static;

    /// The response type for the service.
    type ResponseType: Clone + Debug + Send + Sync + 'static;

    /// The stream type for the service.
    type StreamType: Stream;

    /// Creates a new service.
    async fn new<X>(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
    ) -> Result<Self, Self::Error>
    where
        X: ServiceHandler<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// Sends a request to the service and returns a response.
    async fn request(&self, request: Self::Type) -> Result<Self::ResponseType, Self::Error>;
}
