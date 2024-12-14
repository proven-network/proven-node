#![allow(clippy::or_fun_call)]

mod error;

use crate::service_responder::{MemoryServiceResponder, MemoryUsedServiceResponder};
use crate::subject::MemorySubject;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::service::{Service, ServiceOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use tokio::sync::Mutex;
use uuid::Uuid;

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

    type Responder = MemoryServiceResponder<
        T,
        D,
        S,
        X::ResponseType,
        X::ResponseDeserializationError,
        X::ResponseSerializationError,
    >;

    type UsedResponder = MemoryUsedServiceResponder;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let last_seq = Arc::new(Mutex::new(0));

        // In-memory never has anythong to catch up on.
        handler.on_caught_up().await.unwrap();

        let mut messages = stream.messages().await;

        let last_seq_clone = last_seq.clone();
        let stream_clone = stream.clone();
        tokio::spawn(async move {
            while let Some(message) = messages.next().await {
                let responder = MemoryServiceResponder::new(
                    MemorySubject::new(format!("{name}_reply")).unwrap(),
                    Uuid::new_v4().to_string(),
                    stream_clone.clone(),
                    0, // TODO: this should be the actual seq.
                );

                handler
                    .handle(message, responder)
                    .await
                    .map_err(|_| Error::Handler)
                    .unwrap();

                let mut seq = last_seq_clone.lock().await;
                *seq += 1;
                drop(seq);

                // TODO: Use actual seq.
                stream_clone.del(0).await.unwrap();
            }
        });

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
