use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Message;

/// Marker trait for subscriber errors
pub trait ConsumerHandlerError: Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait ConsumerHandler<T = Bytes>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: ConsumerHandlerError;

    /// Handles the given data.
    async fn handle(&self, message: Message<T>) -> Result<(), Self::Error>;
}
