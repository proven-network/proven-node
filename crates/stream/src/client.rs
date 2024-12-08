use crate::service::Service;
use crate::stream::Stream;
use proven_messaging::Subject;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for client errors
pub trait ClientError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a client of a service the sends requests.
#[async_trait]
pub trait Client<J, S, X>
where
    Self: Clone + Send + Sync + 'static,
    J: Subject + Clone + Send + Sync + 'static,
    S: Stream<J> + Clone + Send + Sync + 'static,
    X: Service<J, S> + Clone + Send + Sync + 'static,
{
    /// The error type for the client.
    type Error: ClientError;

    /// Sends a request to the service and returns a response.
    async fn request(&self, request: J::Type) -> Result<X::Response, Self::Error>;
}
