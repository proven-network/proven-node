mod error;

pub use error::Error;

use crate::stream::MemoryStream;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::Message;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryConsumerOptions;
impl ConsumerOptions for MemoryConsumerOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug)]
pub struct MemoryConsumer<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    last_seq: Arc<Mutex<u64>>,
    _marker: PhantomData<T>,
}

impl<T> MemoryConsumer<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new NATS consumer.
    async fn process_messages<X>(
        last_seq: Arc<Mutex<u64>>,
        mut receiver_stream: ReceiverStream<Message<<Self as Consumer>::Type>>,
        handler: X,
    ) -> Result<(), Error>
    where
        X: ConsumerHandler<Type = T> + Clone + Send + Sync + 'static,
        X::Type: Clone + Debug + Send + Sync + 'static,
    {
        while let Some(message) = receiver_stream.next().await {
            handler.handle(message).await.map_err(|_| Error::Handler)?;

            let mut seq = last_seq.lock().await;
            *seq += 1;
        }

        Ok(())
    }
}

#[async_trait]
impl<T> Consumer for MemoryConsumer<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    type Options = MemoryConsumerOptions;

    type Type = T;

    type StreamType = MemoryStream<T>;

    async fn new<X>(
        _name: String,
        stream: Self::StreamType,
        _options: MemoryConsumerOptions,
        handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ConsumerHandler<Type = T> + Clone + Send + Sync + 'static,
    {
        let last_seq = Arc::new(Mutex::new(0));

        tokio::spawn(Self::process_messages(
            last_seq.clone(),
            stream.messages().await,
            handler.clone(),
        ));

        Ok(Self {
            last_seq,
            _marker: PhantomData,
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }
}
