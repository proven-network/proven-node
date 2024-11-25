use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for SQLStore errors
pub trait StreamError: Clone + Debug + Error + Send + Sync + 'static {}
pub trait StreamHandlerError: Clone + Debug + Error + Send + Sync + 'static {}

#[async_trait]
pub trait Stream<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: StreamHandlerError,
{
    type Error: StreamError;

    async fn request(&self, subject: String, data: Bytes) -> Result<Bytes, Self::Error>;

    async fn handle(
        &self,
        subject: String,
        handler: impl Fn(Bytes) -> Pin<Box<dyn Future<Output = Result<Bytes, HandlerError>> + Send>>
            + Send
            + Sync,
    ) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait Stream1<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: StreamHandlerError,
{
    type Error: StreamError;
    type Scoped: Stream<HandlerError, Error = Self::Error>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait Stream2<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: StreamHandlerError,
{
    type Error: StreamError;
    type Scoped: Stream1<HandlerError, Error = Self::Error>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait Stream3<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: StreamHandlerError,
{
    type Error: StreamError;
    type Scoped: Stream2<HandlerError, Error = Self::Error>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
