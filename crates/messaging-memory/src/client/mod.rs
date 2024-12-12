mod error;
mod subscription_handler;

use crate::stream::InitializedMemoryStream;
use crate::subject::MemorySubject;
pub use error::Error;
use subscription_handler::ClientSubscriptionHandler;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::client::{Client, ClientOptions};
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::InitializedStream;
use proven_messaging::subject::Subject;
use proven_messaging::Message;
use tokio::sync::{mpsc, Mutex};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemoryClientOptions;
impl ClientOptions for MemoryClientOptions {}

/// A client for an in-memory service.
#[derive(Debug)]
pub struct MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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
    reply_receiver: Arc<Mutex<mpsc::Receiver<Message<X::ResponseType>>>>,
    reply_subject: MemorySubject<X::ResponseType, D, S>,
    stream: <Self as Client<X, T, D, S>>::StreamType,
}

impl<X, T, D, S> Clone for MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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
            reply_receiver: self.reply_receiver.clone(),
            reply_subject: self.reply_subject.clone(),
            stream: self.stream.clone(),
        }
    }
}

#[async_trait]
impl<X, T, D, S> Client<X, T, D, S> for MemoryClient<X, T, D, S>
where
    X: ServiceHandler<T, D, S>,
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

    type Options = MemoryClientOptions;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn new(
        name: String,
        stream: Self::StreamType,
        _options: Self::Options,
        _handler: X,
    ) -> Result<Self, Self::Error> {
        let (sender, receiver) = mpsc::channel::<Message<X::ResponseType>>(100);

        let reply_handler = ClientSubscriptionHandler::new(sender);

        let reply_subject: MemorySubject<X::ResponseType, D, S> =
            MemorySubject::new(format!("{name}_reply")).unwrap();

        reply_subject.subscribe(reply_handler).await.unwrap();

        Ok(Self {
            reply_receiver: Arc::new(Mutex::new(receiver)),
            reply_subject,
            stream,
        })
    }

    async fn request(&self, request: T) -> Result<X::ResponseType, Self::Error> {
        let mut reply_receiver = self.reply_receiver.lock().await;

        self.stream
            .publish(Message {
                headers: None,
                payload: request,
            })
            .await
            .unwrap();

        let reply = reply_receiver.recv().await.unwrap();
        drop(reply_receiver);

        Ok(reply.payload)
    }
}
