mod error;

use bytes::Bytes;
pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::NatsStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsClientOptions;
impl ClientOptions for NatsClientOptions {}

/// A client for an in-memory service.
#[derive(Clone, Debug, Default)]
pub struct NatsClient<T, R>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    _marker: PhantomData<(T, R)>,
}

#[async_trait]
impl<T, R> Client for NatsClient<T, R>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;

    type Options = NatsClientOptions;

    type Type = T;

    type ResponseType = R;

    type StreamType = NatsStream<T>;

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
