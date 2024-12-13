#![allow(clippy::type_complexity)]

mod error;

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
use proven_messaging::Message;
use tokio::sync::watch;

use crate::subject::NatsSubject;

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
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type Options = NatsSubscriptionOptions;

    type Subject = NatsSubject<T, D, S>;

    async fn new(
        subject_string: String,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error<D, S>> {
        let (stop_sender, mut stop_receiver) = watch::channel(());

        let subscription = Self {
            stop_sender,
            _marker: PhantomData,
        };

        let mut subscriber = options
            .client
            .subscribe(subject_string.clone())
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
}
