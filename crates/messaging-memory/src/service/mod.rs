mod error;

use crate::subject::MemorySubject;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::subject::PublishableSubject;
use proven_messaging::Message;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::stream::InitializedMemoryStream;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryServiceOptions;
impl ServiceOptions for MemoryServiceOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemoryService<X, T, D, S>
where
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
    stream: <Self as Service<X, T, D, S>>::StreamType,
}

impl<X, T, D, S> Clone for MemoryService<X, T, D, S>
where
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

impl<X, T, D, S> MemoryService<X, T, D, S>
where
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
        reply_subject: MemorySubject<X::ResponseType, D, S>,
        mut receiver_stream: ReceiverStream<Message<T>>,
        handler: X,
    ) -> Result<(), Error> {
        while let Some(message) = receiver_stream.next().await {
            let result = handler.handle(message).await.map_err(|_| Error::Handler)?;

            reply_subject.publish(result).await.unwrap();

            let mut seq = last_seq.lock().await;
            *seq += 1;
        }

        Ok(())
    }
}

#[async_trait]
impl<X, T, D, S> Service<X, T, D, S> for MemoryService<X, T, D, S>
where
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

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let last_seq = Arc::new(Mutex::new(0));

        tokio::spawn(Self::process_messages(
            last_seq.clone(),
            MemorySubject::new(format!("{name}_reply")).unwrap(),
            stream.messages().await,
            handler.clone(),
        ));

        handler.on_caught_up().await.unwrap();

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
