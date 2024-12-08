mod error;

pub use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

type CancelResultChannel<DE, SE> = oneshot::Sender<Result<(), Error<DE, SE>>>;

/// Options for new NATS subscribers.
#[derive(Clone, Debug)]
pub struct NatsSubscriberOptions {
    /// The NATS client to use.
    pub client: Client,
}
impl SubscriptionOptions for NatsSubscriberOptions {}

/// A NATS-based subscriber
#[derive(Clone, Debug)]
pub struct NatsSubscriber<X, T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: SubscriptionHandler<T, DE, SE>,
{
    cancel_result_channel: Arc<Mutex<Option<CancelResultChannel<DE, SE>>>>,
    cancel_token: CancellationToken,
    handler: X,
    last_message: Arc<Mutex<Option<T>>>,
    _marker: std::marker::PhantomData<DE>,
    _marker2: std::marker::PhantomData<SE>,
}

impl<X, T, DE, SE> NatsSubscriber<X, T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: SubscriptionHandler<T, DE, SE>,
{
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
impl<X, T, DE, SE> Subscription<X, T, DE, SE> for NatsSubscriber<X, T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = DE>
        + TryInto<Bytes, Error = SE>
        + 'static,
    X: SubscriptionHandler<T, DE, SE>,
{
    type Error = Error<DE, SE>;
    type Options = NatsSubscriberOptions;

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
            _marker: std::marker::PhantomData,
            _marker2: std::marker::PhantomData,
        };

        let mut subscriber = options
            .client
            .subscribe(subject_string.clone())
            .await
            .map_err(|_| Error::<DE, SE>::Subscribe)?;

        let subscription_clone = subscription.clone();
        let token = subscription.cancel_token.clone();

        tokio::spawn(async move {
            tokio::select! {
                () = token.cancelled() => {
                    let result = subscriber.unsubscribe().await.map_err(|_| Error::<DE, SE>::Unsubscribe);

                    subscription_clone.cancel_result_channel.lock().await.take().unwrap().send(result).unwrap();
                }
                message = subscriber.next() => {
                    if let Some(msg) = message {
                        let data: T = msg
                            .payload
                            .try_into()
                            .map_err(|e| Error::<DE, SE>::Deserialize(e))
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
