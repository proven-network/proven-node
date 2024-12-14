mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::subscription_responder::SubscriptionResponder;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct ClientSubscriptionHandler<T, D, S> {
    sender: mpsc::Sender<T>,
    _marker: std::marker::PhantomData<(D, S)>,
}

impl<T, D, S> Clone for ClientSubscriptionHandler<T, D, S> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> ClientSubscriptionHandler<T, D, S>
where
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
    pub const fn new(sender: mpsc::Sender<T>) -> Self {
        Self {
            sender,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> SubscriptionHandler<T, D, S> for ClientSubscriptionHandler<T, D, S>
where
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

    type ResponseType = T;
    type ResponseDeserializationError = D;
    type ResponseSerializationError = S;

    async fn handle<R>(&self, message: T, responder: R) -> Result<R::UsedResponder, Self::Error>
    where
        R: SubscriptionResponder<Self::ResponseType, D, S>,
    {
        self.sender.send(message).await.map_err(|_| Error::Send)?;

        Ok(responder.no_reply().await)
    }
}
