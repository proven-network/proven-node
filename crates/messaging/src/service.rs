use crate::client::Client;
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
pub trait Service<X, T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    /// The error type for the service.
    type Error: ServiceError;

    /// The options for the service.
    type Options: ServiceOptions;

    /// The stream type for the service.
    type StreamType: Stream<T>;

    /// The client type for the service.
    type ClientType: Client<X, Self::StreamType, Self, T>;

    /// Creates a new subscriber.
    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>;

    /// Gets the client for the service.
    fn client(&self) -> Self::ClientType;

    /// Gets the handler for the service.
    fn handler(&self) -> X;

    /// Gets the last sequence number processed by the service.
    async fn last_seq(&self) -> Result<u64, Self::Error>;

    /// Gets the stream for the service.
    fn stream(&self) -> Self::StreamType;
}
