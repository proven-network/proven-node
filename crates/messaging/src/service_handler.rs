use crate::{Message, ServiceResponse};

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait ServiceHandler<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
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
    /// The error type for the subscriber.
    type Error: Error + Send + Sync + 'static;

    /// The response type for the subscriber.
    type ResponseType: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static;

    /// Handles the given data.
    async fn handle(
        &self,
        message: Message<T>,
    ) -> Result<ServiceResponse<Self::ResponseType>, Self::Error>;

    /// Hook for when the consumer is caught up.
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
