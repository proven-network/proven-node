use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for service errors
pub trait ServiceError: Error + Send + Sync + 'static {}

/// A trait representing a service that handles ordered requests.
#[async_trait]
pub trait Service<S, T>
where
    Self: Clone + Send + Sync + 'static,
    S: Stream<T>,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the handler.
    type Error: ServiceError;

    /// The response type for the handler.
    type Response: Clone + Debug + Send + Sync + 'static;

    /// Handles the given data and returns a response.
    async fn handle(&self, data: T) -> Result<Self::Response, Self::Error>;
}
