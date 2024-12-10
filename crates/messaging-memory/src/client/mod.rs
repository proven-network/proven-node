mod error;

use bytes::Bytes;
pub use error::Error;
use proven_messaging::stream::Stream;

use std::error::Error as StdError;
use std::fmt::Debug;

use async_trait::async_trait;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;

use crate::stream::MemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryClientOptions;
impl ClientOptions for MemoryClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct MemoryClient<P, X, T, D, S>
where
    P: Stream<T, D, S>,
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
    stream: <Self as Client<P, X, T, D, S>>::StreamType,
}

impl<P, X, T, D, S> Clone for MemoryClient<P, X, T, D, S>
where
    P: Stream<T, D, S>,
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
impl<P, X, T, D, S> Client<P, X, T, D, S> for MemoryClient<P, X, T, D, S>
where
    P: Stream<T, D, S>,
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

    type Options = MemoryClientOptions;

    type StreamType = MemoryStream<T, D, S>;

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
