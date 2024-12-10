mod error;

use crate::stream::NatsStream;
use bytes::Bytes;
pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use tokio::sync::Mutex;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct NatsServiceOptions;
impl ServiceOptions for NatsServiceOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug)]
pub struct NatsService<T, R>
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
    last_seq: Arc<Mutex<u64>>,
    stream: <Self as Service>::StreamType,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T, R> Service for NatsService<T, R>
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

    type Options = NatsServiceOptions;

    type Type = T;

    type ResponseType = R;

    type StreamType = NatsStream<Self::Type>;

    async fn new<X>(
        _name: String,
        stream: Self::StreamType,
        _options: NatsServiceOptions,
        _handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ServiceHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
        X::Type: Clone + Debug + Send + Sync + 'static,
        X::ResponseType: Clone + Debug + Send + Sync + 'static,
    {
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
