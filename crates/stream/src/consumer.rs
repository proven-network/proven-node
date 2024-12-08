use crate::stream::Stream;
use proven_messaging::Subject;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait ConsumerError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream.
#[async_trait]
pub trait Consumer<J, S>
where
    Self: Clone + Send + Sync + 'static,
    J: Subject + Clone + Send + Sync + 'static,
    S: Stream<J> + Clone + Send + Sync + 'static,
{
    /// The error type for the consumer.
    type Error: ConsumerError;

    /// Handles the given data.
    async fn handle(&self, data: J::Type) -> Result<(), Self::Error>;
}
