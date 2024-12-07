use std::error::Error;
use std::fmt::Debug;

use crate::Handler;

use async_trait::async_trait;

/// Marker trait for subscriber errors
pub trait SubscriberError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Subscriber<T, X>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: Handler<T>,
{
    /// The error type for the subscriber.
    type Error: SubscriberError;

    /// The handler for the subscriber.
    fn handler(&self) -> X;

    /// Returns the last message received by the subscriber.
    async fn last_message(&self) -> Option<T>;

    /// Creates a new subscriber.
    async fn new(subject_string: String, handler: X) -> Result<Self, Self::Error>;
}
