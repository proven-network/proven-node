mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::consumer::{Consumer, ConsumerOptions};
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::Message;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::stream::InitializedMemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryConsumerOptions;
impl ConsumerOptions for MemoryConsumerOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    handler: X,
    last_seq: Arc<Mutex<u64>>,
    _marker: PhantomData<(T, S, D)>,
}

impl<X, T, D, S> Clone for MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
            handler: self.handler.clone(),
            last_seq: self.last_seq.clone(),
            _marker: PhantomData,
        }
    }
}

impl<X, T, D, S> MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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
    /// Creates a new NATS consumer.
    async fn process_messages(
        last_seq: Arc<Mutex<u64>>,
        mut receiver_stream: ReceiverStream<Message<T>>,
        handler: X,
    ) -> Result<(), Error> {
        while let Some(message) = receiver_stream.next().await {
            handler.handle(message).await.map_err(|_| Error::Handler)?;

            let mut seq = last_seq.lock().await;
            *seq += 1;
        }

        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Consumer<X, T, D, S> for MemoryConsumer<X, T, D, S>
where
    X: ConsumerHandler<T, D, S> + Clone + Debug + Send + Sync + 'static,
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

    type Options = MemoryConsumerOptions;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        _name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let last_seq = Arc::new(Mutex::new(0));

        tokio::spawn(Self::process_messages(
            last_seq.clone(),
            stream.messages().await,
            handler.clone(),
        ));

        Ok(Self {
            handler,
            last_seq,
            _marker: PhantomData,
        })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }
}
