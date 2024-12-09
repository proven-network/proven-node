mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;

use crate::stream::NatsStream;

/// Options for the nats consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumerOptions;
impl ConsumerOptions for NatsConsumerOptions {}

/// A NATS consumer.
#[derive(Clone, Debug)]
pub struct NatsConsumer<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ConsumerHandler<T>,
{
    _handler: X,
    stream: <Self as Consumer<X, T>>::StreamType,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<X, T> Consumer<X, T> for NatsConsumer<X, T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: ConsumerHandler<T>,
{
    type Error = Error<T>;

    type Options = NatsConsumerOptions;

    type StreamType = NatsStream<T>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        stream: Self::StreamType,
        _options: NatsConsumerOptions,
        handler: X,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            _handler: handler,
            stream,
            _marker: PhantomData,
        })
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
