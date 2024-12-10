mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::Stream;
use proven_messaging::Message;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::stream::MemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryServiceOptions;
impl ServiceOptions for MemoryServiceOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemoryService<P, X, T, D, S>
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
}

impl<P, X, T, D, S> Clone for MemoryService<P, X, T, D, S>
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
        }
    }
}

impl<P, X, T, D, S> MemoryService<P, X, T, D, S>
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
    /// Creates a new NATS service.
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
impl<P, X, T, D, S> Service<P, X, T, D, S> for MemoryService<P, X, T, D, S>
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

    type Options = MemoryServiceOptions;

    type StreamType = MemoryStream<T, D, S>;

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

        Ok(Self { last_seq, stream })
    }

    async fn last_seq(&self) -> Result<u64, Self::Error> {
        let seq = self.last_seq.lock().await;
        Ok(*seq)
    }

    fn stream(&self) -> Self::StreamType {
        self.stream.clone()
    }
}
