use crate::consumer_handler::ConsumerHandler;
use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for consumer errors
pub trait ConsumerError: Error + Send + Sync + 'static {}

/// Marker trait for consumer options
pub trait ConsumerOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// The options for the consumer.
    type Options: ConsumerOptions;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync;

    /// The stream type for the consumer.
    type StreamType: Stream;

    /// Creates a new subscriber.
    async fn new<X>(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ConsumerHandler<Type = Self::Type>;

    /// Gets the last sequence number processed by the consumer.
    async fn last_seq(&self) -> Result<u64, Self::Error>;
}
