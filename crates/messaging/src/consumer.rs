use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait ConsumerError: Error + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer<S, T>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream<T>,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// Handles the given data.
    async fn handle(&self, data: T) -> Result<(), Self::Error>;
}
