mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::client::Client;
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::MemoryStream;

/// A client for an in-memory service.
#[derive(Clone, Debug, Default)]
pub struct MemoryClient<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    _marker: PhantomData<(X, T)>,
}

impl<X, T> MemoryClient<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    /// Creates a new in-memory client.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<X, T> Client<X, T> for MemoryClient<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    type Error = Error;

    type StreamType = MemoryStream<T>;

    async fn request(&self, _request: T) -> Result<X::ResponseType, Self::Error> {
        unimplemented!()
    }
}
