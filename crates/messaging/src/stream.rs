use crate::consumer::Consumer;
use crate::service::Service;
use crate::subject::Subject;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for stream errors
pub trait StreamError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a stream.
#[async_trait]
pub trait Stream<J>
where
    Self: Clone + Send + Sync + 'static,
    J: Subject + Clone + Send + Sync + 'static,
    J::Type: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the handler.
    type Error: StreamError;

    /// The subject type for the stream.
    type Subject: Subject = J;

    /// The last message in the stream.
    async fn last_message(&self) -> Result<Option<J::Type>, Self::Error>;

    /// Returns the name of the stream.
    async fn name(&self) -> String;

    /// Consumes the stream with the given consumer.
    async fn start_consumer<C>(&self, consumer: C) -> Result<(), Self::Error>
    where
        C: Consumer<J, Self>;

    /// Consumes the stream with the given service.
    async fn start_service<S>(&self, service: S) -> Result<(), Self::Error>
    where
        S: Service<J, Self>;
}
