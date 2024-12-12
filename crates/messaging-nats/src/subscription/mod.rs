#![allow(clippy::type_complexity)]

mod error;

pub use error::Error;
use proven_messaging::Message;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::subject::Subject;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

/// Options for new NATS subscribers.
#[derive(Clone, Debug)]
pub struct NatsSubscriptionOptions {
    /// The NATS client to use.
    pub client: Client,
}
impl SubscriptionOptions for NatsSubscriptionOptions {}

/// A NATS-based subscriber
#[derive(Debug)]
pub struct NatsSubscription<P, X, T, D, S>
where
    P: Subject<T, D, S>,
    X: SubscriptionHandler<T, D, S>,
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
    cancel_result_channel: Arc<Mutex<Option<oneshot::Sender<Result<(), Error<D, S>>>>>>,
    cancel_token: CancellationToken,
    _marker: PhantomData<(P, X, T)>,
}

impl<P, X, T, D, S> Clone for NatsSubscription<P, X, T, D, S>
where
    P: Subject<T, D, S>,
    X: SubscriptionHandler<T, D, S>,
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
            cancel_result_channel: self.cancel_result_channel.clone(),
            cancel_token: self.cancel_token.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, X, T, D, S> Subscription<P, X, T, D, S> for NatsSubscription<P, X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    P: Subject<T, D, S>,
    X: SubscriptionHandler<T, D, S>,
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
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type Options = NatsSubscriptionOptions;

    type Subject = P;

    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error<D, S>> {
        let subscription = Self {
            cancel_result_channel: Arc::new(Mutex::new(None)),
            cancel_token: CancellationToken::new(),
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
                                .map_err(Error::<D, S>::Deserialize)
                                .unwrap();

                            let message = Message {
                                headers: msg.headers,
                                payload: data,
                            };

                            let _ =handler
                                .handle(message.clone())
                                .await;

                        }
                    }
                }
            }
        });

        Ok(subscription)
    }

    async fn cancel(self) -> Result<(), Self::Error<D, S>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.cancel_result_channel.lock().await.replace(sender);
        self.cancel_token.cancel();

        receiver.await.unwrap()
    }
}
