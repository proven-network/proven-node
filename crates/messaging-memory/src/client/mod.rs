mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::MemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryClientOptions;
impl ClientOptions for MemoryClientOptions {}

/// A client for an in-memory service.
#[derive(Clone, Debug, Default)]
pub struct MemoryClient<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    _marker: PhantomData<(T, R)>,
}

#[async_trait]
impl<T, R> Client for MemoryClient<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    type Options = MemoryClientOptions;

    type Type = T;

    type ResponseType = R;

    type StreamType = MemoryStream<T>;

    async fn new<X>(
        _name: String,
        _stream: Self::StreamType,
        _options: Self::Options,
    ) -> Result<Self, Self::Error>
    where
        X: ServiceHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
        X::Type: Clone + Debug + Send + Sync + 'static,
        X::ResponseType: Clone + Debug + Send + Sync + 'static,
    {
        Ok(Self {
            _marker: PhantomData,
        })
    }

    async fn request(&self, _request: T) -> Result<Self::ResponseType, Self::Error> {
        unimplemented!()
    }
}
