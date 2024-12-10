use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Message;

/// Marker trait for subscriber errors
pub trait ConsumerHandlerError: Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait ConsumerHandler
where
    Self: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: ConsumerHandlerError;

    /// The type of data expected on the subscribed subject.
    type Type: Clone + Debug + Send + Sync + 'static = Bytes;

    /// Handles the given data.
    async fn handle(&self, message: Message<Self::Type>) -> Result<(), Self::Error>;

    /// Hook for when the consumer is caught up.
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
