use crate::service_handler::ServiceHandler;
use crate::stream::InitializedStream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

/// Marker trait for client errors
pub trait ClientError: Error + Send + Sync + 'static {}

/// Marker trait for client options
pub trait ClientOptions: Clone + Debug + Send + Sync + 'static {}

/// A response stream.
pub type ResponseStream<R> = Box<dyn Send + Stream<Item = R> + Unpin>;

/// The response type for a request.
pub enum ClientResponseType<R> {
    /// A single response.
    Response(R),

    /// A stream of responses.
    Stream(ResponseStream<R>),
}

impl<R> Debug for ClientResponseType<R>
where
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(response) => write!(f, "Response({response:?})"),
            Self::Stream(_) => write!(f, "Stream"),
        }
    }
}

/// A trait representing a client of a service the sends requests.
#[async_trait]
pub trait Client<X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the client.
    type Error: ClientError;

    /// The options for the service.
    type Options: ClientOptions;

    /// The response type for the client.
    type ResponseType = X::ResponseType;

    /// The stream type for the client.
    type StreamType: InitializedStream<T, D, S>;

    /// Creates a new service.
    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
    ) -> Result<Self, Self::Error>;

    /// Sends a request to the service and returns a response.
    async fn request(&self, request: T)
    -> Result<ClientResponseType<X::ResponseType>, Self::Error>;
}
