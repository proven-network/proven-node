mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::client::Client;
use proven_messaging::service_handler::ServiceHandler;

use crate::service::MemoryService;
use crate::stream::MemoryStream;

/// A client for an in-memory service.
#[derive(Clone, Debug, Default)]
pub struct MemoryClient<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T, R> Client<T, R> for MemoryClient<T, R>
where
    R: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    type ResponseType = R;

    type ServiceType = MemoryService<T>;

    type StreamType = MemoryStream<T>;

    async fn new<X>(
        _name: String,
        _stream: Self::StreamType,
        _handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ServiceHandler<T>,
    {
        Ok(MemoryClient {
            _marker: PhantomData,
        })
    }

    async fn request(&self, _request: T) -> Result<Self::ResponseType, Self::Error> {
        unimplemented!()
    }
}
