mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::client::Client;
use proven_messaging::service::Service;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::Stream;

/// A client for an in-memory service.
#[derive(Clone, Debug)]
pub struct NatsClient<X, S, V, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    S: Stream<T>,
    X: ServiceHandler<T>,
    V: Service<X, T>,
{
    _service: V,
    _marker: PhantomData<(X, S, T)>,
}

impl<X, S, V, T> NatsClient<X, S, V, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    S: Stream<T>,
    X: ServiceHandler<T>,
    V: Service<X, T>,
{
    /// Creates a new in-memory client.
    pub const fn new(service: V) -> Self {
        Self {
            _service: service,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<X, S, V, T> Client<X, S, V, T> for NatsClient<X, S, V, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    S: Stream<T>,
    X: ServiceHandler<T>,
    V: Service<X, T>,
{
    type Error = Error;

    async fn request(&self, _request: T) -> Result<X::ResponseType, Self::Error> {
        unimplemented!()
    }
}
