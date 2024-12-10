mod error;

use crate::stream::MemoryStream;
pub use error::Error;

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
pub struct MemoryConsumer<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    last_seq: Arc<Mutex<u64>>,
    stream: <Self as Consumer>::StreamType,
    _marker: PhantomData<T>,
}

impl<T, R> MemoryConsumer<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new NATS consumer.
    async fn process_messages<X>(
        last_seq: Arc<Mutex<u64>>,
        mut receiver_stream: ReceiverStream<Message<<Self as Consumer>::Type>>,
        handler: X,
    ) -> Result<(), Error>
    where
        X: ConsumerHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
        X::Type: Clone + Debug + Send + Sync + 'static,
        X::ResponseType: Clone + Debug + Send + Sync + 'static,
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
impl<T, R> Consumer for MemoryConsumer<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    type Options = MemoryConsumerOptions;

    type Type = T;

    type ResponseType = R;

    type StreamType = MemoryStream<Self::Type, Self::ResponseType>;

    async fn new<X>(
        _name: String,
        stream: Self::StreamType,
        _options: MemoryConsumerOptions,
        handler: X,
    ) -> Result<Self, Self::Error>
    where
        X: ConsumerHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
        X::Type: Clone + Debug + Send + Sync + 'static,
        X::ResponseType: Clone + Debug + Send + Sync + 'static,
    {
        let last_seq = Arc::new(Mutex::new(0));

        tokio::spawn(Self::process_messages(
            last_seq.clone(),
            stream.messages().await,
            handler.clone(),
        ));

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