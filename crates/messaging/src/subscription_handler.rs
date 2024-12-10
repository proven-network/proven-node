use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

use crate::Message;

/// Marker trait for subscriber errors
pub trait SubscriptionHandlerError: Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait SubscriptionHandler<T = Bytes, R = Bytes>
where
    Self: Clone + Debug + Send + Sync + 'static,
    Self::Type: Clone + Debug + Send + Sync + 'static,
    Self::ResponseType: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: SubscriptionHandlerError;

    /// The type of data expected on the subscribed subject.
    type Type: Clone + Debug + Send + Sync + 'static = T;

    /// The response type for the subscriber.
    type ResponseType: Clone + Debug + Send + Sync + 'static = R;

    /// Handles the given data.
    async fn handle(&self, message: Message<Self::Type>) -> Result<(), Self::Error>;

    /// Handles the given data and responds with a message.
    async fn respond(
        &self,
        message: Message<Self::Type>,
    ) -> Result<Message<Self::ResponseType>, Self::Error>;
}
