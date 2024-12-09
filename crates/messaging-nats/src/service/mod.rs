mod error;

use crate::client::NatsClient;
use crate::stream::NatsStream;
pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsServiceOptions;
impl ServiceOptions for NatsServiceOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug)]
pub struct NatsService<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ServiceHandler<T>,
{
    handler: X,
    stream: <Self as Service<X, T>>::StreamType,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<X, T> Service<X, T> for NatsService<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ServiceHandler<T>,
{
    type ClientType = NatsClient<X, T>;

    type Error = Error<X::Error>;

    type Options = NatsServiceOptions;

    type StreamType = NatsStream<T>;

    async fn new(
        _name: String,
        _stream: Self::StreamType,
        _options: NatsServiceOptions,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        unimplemented!()
    }

    fn client(&self) -> Self::ClientType {
        unimplemented!()
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        unimplemented!()
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
