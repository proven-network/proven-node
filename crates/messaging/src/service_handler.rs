use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Message;

/// Marker trait for subscriber errors
pub trait ServiceHandlerError: Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait ServiceHandler
where
    Self: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: ServiceHandlerError;

    /// The type of data handled by the service.
    type Type: Clone + Debug + Send + Sync + 'static;

    /// The response type for the service.
    type ResponseType: Clone + Debug + Send + Sync + 'static = Bytes;

    /// Handles the given data.
    async fn handle(
        &self,
        message: Message<Self::Type>,
    ) -> Result<Message<Self::ResponseType>, Self::Error>;

    /// Hook for when the consumer is caught up.
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
