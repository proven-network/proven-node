mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;

use crate::stream::MemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryConsumerOptions;
impl ConsumerOptions for MemoryConsumerOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug)]
pub struct MemoryConsumer<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T>,
{
    _handler: X,
    stream: <Self as Consumer<X, T>>::StreamType,
    _marker: PhantomData<T>,
}

#[async_trait]
impl<X, T> Consumer<X, T> for MemoryConsumer<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T>,
{
    type Error = Error<T>;

    type Options = MemoryConsumerOptions;

    type StreamType = MemoryStream<T>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        stream: Self::StreamType,
        _options: MemoryConsumerOptions,
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
