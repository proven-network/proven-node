use crate::stream::Stream;
use proven_messaging::Subject;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for service errors
pub trait ServiceError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a service that handles ordered requests.
#[async_trait]
pub trait Service<J, S>
where
    Self: Clone + Send + Sync + 'static,
    J: Subject + Clone + Send + Sync + 'static,
    S: Stream<J> + Clone + Send + Sync + 'static,
{
    /// The error type for the handler.
    type Error: ServiceError;

    /// The response type for the handler.
    type Response: Clone + Debug + Send + Sync + 'static;

    /// Handles the given data and returns a response.
    async fn handle(&self, data: J::Type) -> Result<Self::Response, Self::Error>;
}
