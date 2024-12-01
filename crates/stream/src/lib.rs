use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

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
pub trait StreamHandler<S>: Clone + Send + Sync + 'static
where
    S: Stream<Self>,
{
    type HandlerError: StreamHandlerError;

    async fn handle_request(&self, data: S::Request) -> Result<S::Response, Self::HandlerError>;

    async fn on_caught_up(&self) -> Result<(), Self::HandlerError> {
        Ok(())
    }
}

/// A trait representing a stream with asynchronous operations.
///
/// # Associated Types
/// - `Error`: The error type for the handler that implements `Debug`, `Error`, `Send`, and `Sync`.
/// - `Request`: The request type that implements `Clone`, `Send`, and `Sync`.
/// - `Response`: The response type that implements `Clone`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn handle(&self, handler: Handler) -> Result<(), Self::Error>`: Handles the stream with the given handler.
/// - `name`: Returns the name of the stream.
/// - `async fn publish(&self, data: Bytes) -> Result<(), Self::Error>`: Publishes the given data with no expectation of a response.
/// - `async fn request(&self, data: Bytes) -> Result<Bytes, Self::Error>`: Sends a request with the given data and returns the response.
#[async_trait]
pub trait Stream<H>: Clone + Send + Sync + 'static
where
    H: StreamHandler<Self>,
{
    type Error: StreamError;
    type Request: Clone + Send + Sync;
    type Response: Clone + Send + Sync;

    async fn handle(&self, handler: H) -> Result<(), Self::Error>;

    fn name(&self) -> String;

    async fn publish(&self, data: Self::Request) -> Result<(), Self::Error>;

    async fn request(&self, data: Self::Request) -> Result<Self::Response, Self::Error>;
}

#[async_trait]
pub trait Stream1<H>: Clone + Send + Sync + 'static
where
    H: StreamHandler<Self::Scoped>,
{
    type Error: StreamError;
    type Request: Clone + Send + Sync;
    type Response: Clone + Send + Sync;
    type Scoped: Stream<H, Error = Self::Error, Request = Self::Request, Response = Self::Response>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait Stream2<H>: Clone + Send + Sync + 'static
where
    H: StreamHandler<<Self::Scoped as Stream1<H>>::Scoped>,
{
    type Error: StreamError;
    type Request: Clone + Send + Sync;
    type Response: Clone + Send + Sync;
    type Scoped: Stream1<H, Error = Self::Error, Request = Self::Request, Response = Self::Response>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait Stream3<H>: Clone + Send + Sync + 'static
where
    H: StreamHandler<<<Self::Scoped as Stream2<H>>::Scoped as Stream1<H>>::Scoped>,
{
    type Error: StreamError;
    type Request: Clone + Send + Sync;
    type Response: Clone + Send + Sync;
    type Scoped: Stream2<H, Error = Self::Error, Request = Self::Request, Response = Self::Response>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
