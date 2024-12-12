mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::InitializedNatsStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsClientOptions;
impl ClientOptions for NatsClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    stream: <Self as Client<X, T, D, S>>::StreamType,
}

impl<X, T, D, S> Clone for NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

#[async_trait]
impl<X, T, D, S> Client<X, T, D, S> for NatsClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;

    type Options = NatsClientOptions;

    type StreamType = InitializedNatsStream<T, D, S>;

    async fn new(
        _name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        Ok(Self { stream })
    }

    async fn request(&self, _request: T) -> Result<X::ResponseType, Self::Error> {
        unimplemented!()
    }
}
