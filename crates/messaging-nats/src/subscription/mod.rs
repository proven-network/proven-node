#![allow(clippy::type_complexity)]

mod error;

use crate::subject::NatsUnpublishableSubject;
use crate::subscription_responder::{NatsSubscriptionResponder, NatsUsedSubscriptionResponder};
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::watch;

/// Options for new NATS subscribers.
#[derive(Clone, Debug)]
pub struct NatsSubscriptionOptions {
    /// The NATS client to use.
    pub client: Client,
}
impl SubscriptionOptions for NatsSubscriptionOptions {}

/// A NATS-based subscriber
#[derive(Debug)]
pub struct NatsSubscription<X, T, D, S>
where
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
    stop_sender: watch::Sender<()>,
    _marker: PhantomData<(X, T)>,
}

impl<X, T, D, S> Clone for NatsSubscription<X, T, D, S>
where
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
            stop_sender: self.stop_sender.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<X, T, D, S> Subscription<X, T, D, S> for NatsSubscription<X, T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
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
    type Error = Error;

    type Options = NatsSubscriptionOptions;

    type Subject = NatsUnpublishableSubject<T, D, S>;

    async fn new(
        subject: Self::Subject,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let (stop_sender, mut stop_receiver) = watch::channel(());

        let subscription = Self {
            stop_sender,
            _marker: PhantomData,
        };

        let mut subscriber = options
            .client
            .subscribe(String::from(subject))
            .await
            .map_err(|_| Error::Subscribe)?;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_receiver.changed() => {
                        break;
                    }
                    message = subscriber.next() => {
                        if let Some(msg) = message {
                            let data: T = msg.payload
                                .try_into()
                                .map_err(|e: D| Error::Deserialize(e.to_string()))
                                .unwrap();

                                let responder = NatsSubscriptionResponder::new(options.client.clone(), "TODO".to_string(), "TODO".to_string());

                                let _: NatsUsedSubscriptionResponder = handler.handle(data.clone(), responder).await.unwrap();
                        }
                    }
                }
            }
        });

        Ok(subscription)
    }
}
