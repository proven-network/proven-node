//! Abstract interface for managing distributed streams.
#![feature(associated_type_defaults)]
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
pub struct HandlerResponse<T>
where
    T: Clone + Debug + Send + Sync + TryFrom<Bytes> + TryInto<Bytes> + 'static,
{
    /// The response headers
    pub headers: HashMap<String, String>,

    /// The response data
    pub data: T,
}

/// A trait for handling stream data with asynchronous operations.
#[async_trait]
pub trait StreamHandler
where
    Self: Clone + Send + Sync + 'static,
    Self::Request: Clone + Debug + Send + Sync + TryFrom<Bytes> + TryInto<Bytes> + 'static,
    Self::Response: Clone + Debug + Send + Sync + TryFrom<Bytes> + TryInto<Bytes> + 'static,
{
    /// The error type for the handler.
    type Error: StreamHandlerError;

    /// The request type for the handler.
    type Request: Clone + Debug + Send + Sync + TryFrom<Bytes> + TryInto<Bytes> + 'static;

    /// The response type for the handler.
    type Response: Clone + Debug + Send + Sync + TryFrom<Bytes> + TryInto<Bytes> + 'static;

    /// Handles the given data and returns a response.
    async fn handle(
        &self,
        data: Self::Request,
    ) -> Result<HandlerResponse<Self::Response>, Self::Error>;

    /// Hook for when the stream is caught up.
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// A trait representing a stream with asynchronous operations.
#[async_trait]
pub trait Stream<H>
where
    Self: Clone + Send + Sync + 'static,
    H: StreamHandler,
{
    /// The error type for the stream.
    type Error: StreamError;

    /// Begins consuming the stream with the given handler.
    async fn handle(&self, handler: H) -> Result<(), Self::Error>;

    /// Returns the last message in the stream.
    async fn last_message(&self) -> Result<Option<H::Request>, Self::Error>;

    /// Returns the name of the stream.
    fn name(&self) -> String;

    /// Publishes the given data with no expectation of a response.
    async fn publish(&self, data: H::Request) -> Result<(), Self::Error>;

    /// Sends a request with the given data and returns the response.
    async fn request(&self, data: H::Request) -> Result<H::Response, Self::Error>;
}

macro_rules! define_scoped_stream {
    ($index:expr, $parent:ident, $doc:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc]
            pub trait [< Stream $index >]<H>
            where
                H: StreamHandler,
            {
                /// The error type for the stream.
                type Error: StreamError;

                /// The scoped version of the stream.
                type Scoped: $parent<H, Error = Self::Error> + Clone + Send + Sync + 'static;

                /// Creates a scoped version of the stream.
                fn scope<S: Into<String> + Send>(&self, scope: S) -> <Self as [< Stream $index >]<H>>::Scoped;
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
