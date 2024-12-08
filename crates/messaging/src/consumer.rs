use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait ConsumerError: Error + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer<S, T, DE, SE>
where
    Self: Clone + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
    S: Stream<T, DE, SE>,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// Handles the given data.
    async fn handle(&self, data: T) -> Result<(), Self::Error>;
}
