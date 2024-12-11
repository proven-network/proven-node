mod error;

use crate::stream::NatsStream;
use bytes::Bytes;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::Stream;
use tokio::sync::Mutex;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsServiceOptions;
impl ServiceOptions for NatsServiceOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct NatsService<P, X, T, D, S>
where
    P: Stream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    last_seq: Arc<Mutex<u64>>,
    stream: <Self as Service<P, X, T, D, S>>::StreamType,
    _marker: PhantomData<T>,
}

impl<P, X, T, D, S> Clone for NatsService<P, X, T, D, S>
where
    P: Stream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
            last_seq: self.last_seq.clone(),
            stream: self.stream.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, X, T, D, S> Service<P, X, T, D, S> for NatsService<P, X, T, D, S>
where
    P: Stream<T, D, S> + Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

    type Options = NatsServiceOptions;

    type StreamType = NatsStream<T, D, S>;

    async fn new(
        _name: String,
        stream: Self::StreamType,
        _options: NatsServiceOptions,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        let last_seq = Arc::new(Mutex::new(0));

        Ok(Self {
            last_seq,
            stream,
            _marker: PhantomData,
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
