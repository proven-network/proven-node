use crate::subject::Subject;
use crate::subscription_handler::SubscriptionHandler;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for subscriber errors
pub trait SubscriptionError: Error + Send + Sync + 'static {}

/// Marker trait for subscriber options
pub trait SubscriptionOptions: Clone + Debug + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Subscription<X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
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
    type Error: SubscriptionError;

    /// The options for the subscriber.
    type Options: SubscriptionOptions;

    /// The subject type for the subscriber.
    type Subject: Subject<T, D, S>;

    /// Creates a new subscriber.
    async fn new(
        subject: Self::Subject,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>;
}
