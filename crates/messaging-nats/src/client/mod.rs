mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::Client;
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::NatsStream;

/// A client for an in-memory service.
#[derive(Clone, Debug, Default)]
pub struct NatsClient<X, T>
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
    _marker: PhantomData<(X, T)>,
}

impl<X, T> NatsClient<X, T>
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
    /// Creates a new in-memory client.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<X, T> Client<X, T> for NatsClient<X, T>
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
    type Error = Error;

    type StreamType = NatsStream<T>;

    async fn request(&self, _request: T) -> Result<X::ResponseType, Self::Error> {
        unimplemented!()
    }
}
