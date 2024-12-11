use crate::consumer_handler::ConsumerHandler;
use crate::stream::InitializedStream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for consumer errors
pub trait ConsumerError: Error + Send + Sync + 'static {}

/// Marker trait for consumer options
pub trait ConsumerOptions: Clone + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer<P, X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    P: InitializedStream<T, D, S>,
    X: ConsumerHandler<T, D, S>,
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
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// The options for the consumer.
    type Options: ConsumerOptions;

    /// The stream type for the consumer.
    type StreamType: InitializedStream<T, D, S>;

    /// Creates a new subscriber.
    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error>;

    /// Gets the last sequence number processed by the consumer.
    async fn last_seq(&self) -> Result<u64, Self::Error>;
}
