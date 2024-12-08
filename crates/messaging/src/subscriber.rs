use std::convert::Infallible;
use std::error::Error;
use std::fmt::Debug;

use crate::Handler;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for subscriber errors
pub trait SubscriberError: Error + Send + Sync + 'static {}

/// Marker trait for subscriber options
pub trait SubscriberOptions: Clone + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Subscriber<X, T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: Handler<T>,
{
    /// The error type for the subscriber.
    type Error: SubscriberError;

    /// The options for the subscriber.
    type Options: SubscriberOptions;

    /// The handler for the subscriber.
    fn handler(&self) -> X;

    /// Returns the last message received by the subscriber.
    async fn last_message(&self) -> Option<T>;

    /// Creates a new subscriber.
    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>;
}
