mod error;

use crate::client::MemoryClient;
use crate::stream::MemoryStream;
pub use error::Error;

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
pub struct MemoryService<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    last_seq: Arc<Mutex<u64>>,
    handler: X,
    stream: <Self as Service<X, T>>::StreamType,
    _marker: PhantomData<T>,
}

impl<X, T> MemoryService<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
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
impl<X, T> Service<X, T> for MemoryService<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T>,
{
    type ClientType = MemoryClient<X, T>;

    type Error = Error<X::Error>;

    type Options = MemoryServiceOptions;

    type StreamType = MemoryStream<T>;

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

    fn client(&self) -> Self::ClientType {
        unimplemented!()
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
