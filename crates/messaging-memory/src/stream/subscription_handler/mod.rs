mod error;

pub use error::Error;

use std::fmt::Debug;

use async_trait::async_trait;
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::Message;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct StreamSubscriptionHandler<T, R> {
    sender: mpsc::Sender<Message<T>>,
    _marker: std::marker::PhantomData<R>,
}

impl<T, R> StreamSubscriptionHandler<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    pub const fn new(sender: mpsc::Sender<Message<T>>) -> Self {
        Self {
            sender,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T, R> SubscriptionHandler for StreamSubscriptionHandler<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    type Type = T;

    type ResponseType = R;

    async fn handle(&self, message: Message<Self::Type>) -> Result<(), Self::Error> {
        self.sender.send(message).await.map_err(Error::Send)
    }

    async fn respond(
        &self,
        message: Message<Self::Type>,
    ) -> Result<Message<Self::ResponseType>, Self::Error> {
        let _ = self.sender.send(message).await.map_err(Error::Send);

        unimplemented!()
    }
}
