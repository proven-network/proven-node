mod error;

use crate::stream::MemoryStream;
pub use error::Error;
use proven_messaging::client::Client;
use proven_messaging::stream::Stream;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::Message;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryServiceOptions;
impl ServiceOptions for MemoryServiceOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug)]
pub struct MemoryService<X, T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
    X::Type: Clone + Debug + Send + Sync + 'static,
    X::ResponseType: Clone + Debug + Send + Sync + 'static,
{
    last_seq: Arc<Mutex<u64>>,
    handler: X,
    stream: <Self as Service<X>>::StreamType,
    _marker: PhantomData<T>,
}

impl<X, T, R> MemoryService<X, T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
    X::Type: Clone + Debug + Send + Sync + 'static,
    X::ResponseType: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new NATS service.
    async fn process_messages(
        last_seq: Arc<Mutex<u64>>,
        mut receiver_stream: ReceiverStream<Message<T>>,
        handler: X,
    ) -> Result<(), Error<X::Error>> {
        while let Some(message) = receiver_stream.next().await {
            handler.handle(message).await.map_err(Error::Handler)?;

            let mut seq = last_seq.lock().await;
            *seq += 1;
        }

        Ok(())
    }
}

#[async_trait]
impl<X, T, R> Service<X> for MemoryService<X, T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
    X::Type: Clone + Debug + Send + Sync + 'static,
    X::ResponseType: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<X::Error>;

    type Options = MemoryServiceOptions;

    type Type = T;

    type ResponseType = R;

    type StreamType = MemoryStream<Self::Type, Self::ResponseType>;

    async fn new(
        _name: String,
        stream: Self::StreamType,
        _options: MemoryServiceOptions,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let last_seq = Arc::new(Mutex::new(0));

        tokio::spawn(Self::process_messages(
            last_seq.clone(),
            stream.messages().await,
            handler.clone(),
        ));

        Ok(Self {
            last_seq,
            handler,
            stream,
            _marker: PhantomData,
        })
    }

    async fn client(&self) -> Client<X, Self::StreamType, T> {
        self.stream.client("put name here").await.unwrap()
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
