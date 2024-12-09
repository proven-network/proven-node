mod error;

pub use error::Error;

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub struct StreamSubscriptionHandler<T> {
    sender: mpsc::Sender<T>,
}

impl<T> StreamSubscriptionHandler<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub const fn new(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl<T> SubscriptionHandler<T> for StreamSubscriptionHandler<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<T>;

    async fn handle(
        &self,
        _subject: String,
        data: T,
        _headers_opt: Option<HashMap<String, String>>,
    ) -> Result<(), Self::Error> {
        self.sender.send(data).await.map_err(Error::Send)
    }
}
