use crate::service::Service;
use crate::service_handler::ServiceHandler;
use crate::stream::Stream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for client errors
pub trait ClientError: Error + Send + Sync + 'static {}

/// A trait representing a client of a service the sends requests.
#[async_trait]
pub trait Client<X, S, V, T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    S: Stream<T>,
    X: ServiceHandler<T>,
    V: Service<X, T>,
{
    /// The error type for the client.
    type Error: ClientError;

    /// Sends a request to the service and returns a response.
    async fn request(&self, request: T) -> Result<X::ResponseType, Self::Error>;
}
