use crate::consumer_handler::ConsumerHandler;
use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait ConsumerError: Error + Send + Sync + 'static {}

/// Marker trait for subscriber options
pub trait ConsumerOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer<X, S, T>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream<T>,
    T: Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T>,
{
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// The options for the consumer.
    type Options: ConsumerOptions;

    /// Creates a new subscriber.
    async fn new(stream: S, options: Self::Options, handler: X) -> Result<Self, Self::Error>;
}
