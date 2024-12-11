use crate::subject::Subject;
use crate::subscription_handler::SubscriptionHandler;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for subscriber errors
pub trait SubscriptionError: Error + Send + Sync + 'static {}

/// Marker trait for subscriber options
pub trait SubscriptionOptions: Clone + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Subscription<P, X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    P: Subject<T, D, S>,
    X: SubscriptionHandler<T, D, S>,
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
    type Error<DE, SE>: SubscriptionError
    where
        DE: Debug + Error + Send + Sync + 'static,
        SE: Debug + Error + Send + Sync + 'static;

    /// The options for the subscriber.
    type Options: SubscriptionOptions;

    /// The subject type for the subscriber.
    type SubjectType: Subject<T, D, S>;

    /// Creates a new subscriber.
    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error<D, S>>;

    /// Cancels the subscription.
    async fn cancel(self) -> Result<(), Self::Error<D, S>>;

    /// The handler for the subscriber.
    fn handler(&self) -> X;

    /// Returns the last message received by the subscriber.
    async fn last_message(&self) -> Option<Message<T>>;
}
