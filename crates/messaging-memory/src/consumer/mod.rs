mod error;

pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::Stream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryConsumerOptions;
impl ConsumerOptions for MemoryConsumerOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug, Default)]
pub struct MemoryConsumer<X, S, T = Bytes> {
    _marker: PhantomData<(X, S, T)>,
}

#[async_trait]
impl<X, S, T> Consumer<X, S, T> for MemoryConsumer<X, S, T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    S: Stream<T>,
    T: Clone + Debug + Send + Sync + 'static,
    X: ConsumerHandler<T>,
{
    type Error = Error;

    type Options = MemoryConsumerOptions;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        _stream: S,
        _options: MemoryConsumerOptions,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}
