mod error;

use crate::subject::MemoryUnpublishableSubject;
use crate::subscription_responder::{MemorySubscriptionResponder, MemoryUsedSubscriptionResponder};
use crate::{GLOBAL_STATE, GlobalState};
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{broadcast, watch};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemorySubscriptionOptions;
impl SubscriptionOptions for MemorySubscriptionOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemorySubscription<X, T, D, S>
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
    _marker: PhantomData<(X, T, D, S)>,
}

impl<X, T, D, S> Clone for MemorySubscription<X, T, D, S>
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
impl<X, T, D, S> Subscription<X, T, D, S> for MemorySubscription<X, T, D, S>
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

    type Options = MemorySubscriptionOptions;

    type Subject = MemoryUnpublishableSubject<T, D, S>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        subject: Self::Subject,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<GlobalState<T>>() {
            state.put(GlobalState::<T>::default());
        }
        let subject_state = state.borrow::<GlobalState<T>>();
        let mut subjects = subject_state.subjects.lock().await;
        let sender = subjects
            .entry(subject.into())
            .or_insert_with(|| broadcast::channel(100).0)
            .clone();

        let mut receiver = sender.subscribe();

        let (stop_sender, stop_receiver) = watch::channel(());

        let subscriber = Self {
            stop_sender,
            _marker: PhantomData,
        };

        tokio::spawn(async move {
            let mut stop_receiver = stop_receiver.clone();
            loop {
                tokio::select! {
                    _ = stop_receiver.changed() => {
                        break;
                    }
                    message = receiver.recv() => {
                        if let Ok(message) = message {
                            let responder = MemorySubscriptionResponder::new("TODO".to_string(), "TODO".to_string());
                            let message = message.clone();
                            let _: MemoryUsedSubscriptionResponder = handler.handle(message.clone(), responder).await.unwrap();
                        }
                    }
                }
            }
        });

        Ok(subscriber)
    }
}
