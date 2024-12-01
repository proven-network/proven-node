use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for SQLStore errors
pub trait StreamError: Clone + Debug + Error + Send + Sync + 'static {}
pub trait StreamHandlerError: Clone + Debug + Error + Send + Sync + 'static {}

#[async_trait]
pub trait StreamHandler<Error>: Clone + Send + Sync + 'static
where
    Error: StreamHandlerError,
{
    async fn handle(&self, data: Bytes) -> Result<Bytes, Error>;
}

/// A trait representing a stream with asynchronous operations.
///
/// # Associated Types
/// - `HandlerError`: The error type for the handler that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn handle(&self, handler: Handler) -> Result<(), Self::Error>`: Handles the stream with the given handler.
/// - `async fn publish(&self, data: Bytes) -> Result<(), Self::Error>`: Publishes the given data with no expectation of a response.
/// - `async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error>`: Sends a request with the given data and returns the response.
#[async_trait]
pub trait Stream<Handler, HandlerError>: Clone + Send + Sync + 'static
where
    Handler: StreamHandler<HandlerError>,
    HandlerError: StreamHandlerError,
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
        pub trait $name<Handler, HandlerError>: Clone + Send + Sync + 'static
        where
            Handler: StreamHandler<HandlerError>,
            HandlerError: StreamHandlerError,
        {
            type Error: StreamError;
            type Scoped: $parent<Handler, HandlerError, Error = Self::Error>;

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
