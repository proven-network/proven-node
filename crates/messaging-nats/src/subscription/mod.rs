mod error;

pub use error::Error;

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

type CancelResultChannel = oneshot::Sender<Result<(), Error>>;

/// Options for new NATS subscribers.
#[derive(Clone, Debug)]
pub struct NatsSubscriptionOptions {
    /// The NATS client to use.
    pub client: Client,
}
impl SubscriptionOptions for NatsSubscriptionOptions {}

/// A NATS-based subscriber
#[derive(Clone, Debug)]
pub struct NatsSubscription<X, T = Bytes> {
    cancel_result_channel: Arc<Mutex<Option<CancelResultChannel>>>,
    cancel_token: CancellationToken,
    handler: X,
    last_message: Arc<Mutex<Option<T>>>,
    _marker: PhantomData<T>,
}

impl<X, T> NatsSubscription<X, T> {
    fn extract_headers(headers: &async_nats::HeaderMap) -> Option<HashMap<String, String>> {
        let mut result = HashMap::new();
        for (key, value) in headers.iter() {
            if let Some(stripped_key) = key.to_string().strip_prefix("Proven-") {
                #[allow(clippy::or_fun_call)]
                result.insert(
                    stripped_key.to_string(),
                    value
                        .first()
                        .unwrap_or(&async_nats::HeaderValue::new())
                        .to_string(),
                );
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

#[async_trait]
impl<X, T> Subscription<X, T> for NatsSubscription<X, T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
    X: SubscriptionHandler<T>,
{
    type Error = Error;

    type Options = NatsSubscriptionOptions;

    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let subscription = Self {
            cancel_result_channel: Arc::new(Mutex::new(None)),
            cancel_token: CancellationToken::new(),
            handler,
            last_message: Arc::new(Mutex::new(None)),
            _marker: PhantomData,
        };

        let mut subscriber = options
            .client
            .subscribe(subject_string.clone())
            .await
            .map_err(|_| Error::Subscribe)?;

        let subscription_clone = subscription.clone();
        let token = subscription.cancel_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = token.cancelled() => {
                        let result = subscriber.unsubscribe().await.map_err(|_| Error::Unsubscribe);

                        subscription_clone.cancel_result_channel.lock().await.take().unwrap().send(result).unwrap();
                    }
                    message = subscriber.next() => {
                        if let Some(msg) = message {
                            let data: T = msg
                                .payload
                                .try_into()
                                .map_err(Error::Deserialize)
                                .unwrap();
                            let headers = msg.headers.as_ref().and_then(Self::extract_headers);

                            let _ = subscription_clone
                                .handler()
                                .handle(subject_string.clone(), data.clone(), headers)
                                .await;

                                subscription_clone.last_message.lock().await.replace(data);
                        }
                    }
                }
            }
        });

        Ok(subscription)
    }

    async fn cancel(self) -> Result<(), Self::Error> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.cancel_result_channel.lock().await.replace(sender);
        self.cancel_token.cancel();

        receiver.await.unwrap()
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_message(&self) -> Option<T> {
        self.last_message.lock().await.clone()
    }
}
