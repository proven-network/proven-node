mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::Message;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct StreamSubscriptionHandler<T, D, S> {
    sender: mpsc::Sender<Message<T>>,
    _marker: std::marker::PhantomData<(D, S)>,
}

impl<T, D, S> Clone for StreamSubscriptionHandler<T, D, S> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> StreamSubscriptionHandler<T, D, S>
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
    pub const fn new(sender: mpsc::Sender<Message<T>>) -> Self {
        Self {
            sender,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> SubscriptionHandler<T, D, S> for StreamSubscriptionHandler<T, D, S>
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

    async fn handle(
        &self,
        message: Message<T>,
    ) -> Result<Option<Message<Self::ResponseType>>, Self::Error> {
        self.sender.send(message).await.map_err(|_| Error::Send)?;

        Ok(None)
    }
}
