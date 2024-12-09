mod error;

pub use error::Error;

use std::fmt::Debug;

use async_trait::async_trait;
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::Message;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct StreamSubscriptionHandler<T> {
    sender: mpsc::Sender<Message<T>>,
}

impl<T> StreamSubscriptionHandler<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub const fn new(sender: mpsc::Sender<Message<T>>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl<T> SubscriptionHandler<T> for StreamSubscriptionHandler<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    async fn handle(&self, _subject: String, message: Message<T>) -> Result<(), Self::Error> {
        self.sender.send(message).await.map_err(Error::Send)
    }
}
