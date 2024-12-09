mod error;

use crate::stream::MemoryStream;
pub use error::Error;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

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
    last_seq: Arc<Mutex<u64>>,
    handler: X,
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

    async fn new(
        stream: Self::StreamType,
        _options: MemoryConsumerOptions,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let handler_clone = handler.clone();
        let mut message_stream = stream.messages().await;
        let last_seq = Arc::new(Mutex::new(0));
        let last_seq_clone = last_seq.clone();

        tokio::spawn(async move {
            while let Some(message) = message_stream.next().await {
                if handler_clone.handle(message).await.is_ok() {
                    let mut seq = last_seq_clone.lock().await;
                    *seq += 1;
                }
            }
        });

        Ok(Self {
            last_seq,
            handler,
            stream,
            _marker: PhantomData,
        })
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
