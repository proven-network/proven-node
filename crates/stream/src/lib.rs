use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for stream errors
pub trait StreamError: Clone + Debug + Error + Send + Sync + 'static {}

/// Marker trait for stream handler errors
pub trait StreamHandlerError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait for handling stream data with asynchronous operations.
///
/// # Type Parameters
/// - `HandlerError`: The error type that implements `StreamHandlerError`
///
/// # Required Methods
/// - `handle`: Process the received data and return a response
#[async_trait]
pub trait StreamHandler: Clone + Send + Sync + 'static {
    type HandlerError: StreamHandlerError;

    async fn handle(&self, data: Bytes) -> Result<Bytes, Self::HandlerError>;
}

/// A trait representing a stream with asynchronous operations.
///
/// # Associated Types
/// - `Error`: The error type for the handler that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn handle(&self, handler: Handler) -> Result<(), Self::Error>`: Handles the stream with the given handler.
/// - `name`: Returns the name of the stream.
/// - `async fn publish(&self, data: Bytes) -> Result<(), Self::Error>`: Publishes the given data with no expectation of a response.
/// - `async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error>`: Sends a request with the given data and returns the response.
#[async_trait]
pub trait Stream<Handler>: Clone + Send + Sync + 'static
where
    Handler: StreamHandler,
{
    type Error: StreamError;

    async fn handle(&self, handler: Handler) -> Result<(), Self::Error>;

    fn name(&self) -> String;

    async fn publish(&self, data: Bytes) -> Result<(), Self::Error>;

    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error>;
}

macro_rules! define_scoped_stream {
    ($name:ident, $parent:ident, $doc:expr) => {
        #[async_trait]
        #[doc = $doc]
        pub trait $name<Handler>: Clone + Send + Sync + 'static
        where
            Handler: StreamHandler,
        {
            type Error: StreamError;
            type Scoped: $parent<Handler, Error = Self::Error>;

            fn scope(&self, scope: String) -> Self::Scoped;
        }
    };
}

define_scoped_stream!(
    Stream1,
    Stream,
    "A trait representing a single-scoped stream with asynchronous operations."
);
define_scoped_stream!(
    Stream2,
    Stream1,
    "A trait representing a double-scoped stream with asynchronous operations."
);
define_scoped_stream!(
    Stream3,
    Stream2,
    "A trait representing a triple-scoped stream with asynchronous operations."
);
