mod error;

pub use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::{Handler, Subscriber, SubscriberOptions};
use tokio::sync::Mutex;

/// Options for new NATS subscribers.
#[derive(Clone, Debug)]
pub struct NatsSubscriberOptions {
    /// The NATS client to use.
    pub client: Client,
}
impl SubscriberOptions for NatsSubscriberOptions {}

/// A NATS-based subscriber
#[derive(Clone, Debug)]
pub struct NatsSubscriber<X, T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    X: Handler<T>,
{
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
    X: Handler<T>,
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
impl<X, T, DE, SE> Subscriber<X, T, DE, SE> for NatsSubscriber<X, T, DE, SE>
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
    X: Handler<T>,
{
    type Error = Error<DE, SE>;
    type Options = NatsSubscriberOptions;

    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let subscriber = Self {
            handler,
            last_message: Arc::new(Mutex::new(None)),
            _marker: std::marker::PhantomData,
            _marker2: std::marker::PhantomData,
        };

        let mut subscription = options
            .client
            .subscribe(subject_string.clone())
            .await
            .map_err(|_| Error::<DE, SE>::Subscribe)?;

        let subscriber_clone = subscriber.clone();
        tokio::spawn(async move {
            while let Some(msg) = subscription.next().await {
                let data: T = msg
                    .payload
                    .try_into()
                    .map_err(|e| Error::<DE, SE>::Deserialize(e))
                    .unwrap();
                let headers = msg.headers.as_ref().and_then(Self::extract_headers);

                let _ = subscriber_clone
                    .handler()
                    .handle(subject_string.clone(), data.clone(), headers)
                    .await;

                subscriber_clone.last_message.lock().await.replace(data);
            }
        });

        Ok(subscriber)
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_message(&self) -> Option<T> {
        self.last_message.lock().await.clone()
    }
}
