//! Abstract interface for managing distributed streams.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for stream errors
pub trait StreamError: Clone + Debug + Error + Send + Sync + 'static {}

/// Marker trait for stream handler errors
pub trait StreamHandlerError: Clone + Debug + Error + Send + Sync + 'static {}

/// A struct representing a handler response.
#[derive(Debug, Default)]
pub struct HandlerResponse {
    /// The response headers
    pub headers: HashMap<String, String>,

    /// The response data
    pub data: Bytes,
}

/// A trait for handling stream data with asynchronous operations.
#[async_trait]
pub trait StreamHandler
where
    Self: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the handler.
    type Error: StreamHandlerError;

    /// Handles the given data and returns a response.
    async fn handle(&self, data: Bytes) -> Result<HandlerResponse, Self::Error>;

    /// Hook for when the stream is caught up.
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A trait representing a stream with asynchronous operations.
#[async_trait]
pub trait Stream<Handler>
where
    Self: Clone + Debug + Send + Sync + 'static,
    Handler: StreamHandler,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// Begins consuming the stream with the given handler.
    async fn handle(&self, handler: Handler) -> Result<(), Self::Error>;

    /// Returns the last message in the stream.
    async fn last_message(&self) -> Result<Option<Bytes>, Self::Error>;

    /// Returns the name of the stream.
    fn name(&self) -> String;

    /// Publishes the given data with no expectation of a response.
    async fn publish(&self, data: Bytes) -> Result<(), Self::Error>;

    /// Sends a request with the given data and returns the response.
    async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error>;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! Stream $index]]

            #[async_trait]
            #[doc = $doc]
            pub trait #name<Handler>
            where
                Handler: StreamHandler,
            {
                /// The error type for the stream.
                type Error: StreamError;

                /// The scoped version of the stream.
                type Scoped: $parent<Handler, Error = Self::Error> + Clone + Debug + Send + Sync;

                /// Creates a scoped version of the stream.
                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> <Self as #name<Handler>>::Scoped;
            }
        }
    };
}

define_scoped_stream!(
    1,
    Stream,
    "A trait representing a single-scoped stream with asynchronous operations."
);
define_scoped_stream!(
    2,
    Stream1,
    "A trait representing a double-scoped stream with asynchronous operations."
);
define_scoped_stream!(
    3,
    Stream2,
    "A trait representing a triple-scoped stream with asynchronous operations."
);
