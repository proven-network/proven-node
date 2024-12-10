use crate::subscription_handler::{SubscriptionHandler, SubscriptionHandlerError};
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for subscriber errors
pub trait SubscriptionError: Error + Send + Sync + 'static {}

/// Marker trait for subscriber options
pub trait SubscriptionOptions: Clone + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Subscription<X>
where
    Self: Clone + Send + Sync + 'static,
    X: SubscriptionHandler<Type = Self::Type, ResponseType = Self::ResponseType>,
{
    /// The error type for the subscriber.
    type Error: SubscriptionError;

    /// The handler error type for the subscriber.
    type HandlerError: SubscriptionHandlerError = X::Error;

    /// The type of data expected on the subscribed subject.
    type Type: Clone + Debug + Send + Sync + 'static;

    /// The response type for the subscriber.
    type ResponseType: Clone + Debug + Send + Sync + 'static;

    /// The options for the subscriber.
    type Options: SubscriptionOptions;

    /// Creates a new subscriber.
    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>;

    /// Cancels the subscription.
    async fn cancel(self) -> Result<(), Self::Error>;

    /// The handler for the subscriber.
    fn handler(&self) -> X;

    /// Returns the last message received by the subscriber.
    async fn last_message(&self) -> Option<Message<Self::Type>>;
}
