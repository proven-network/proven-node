mod error;

use crate::subject::MemorySubject;
use crate::{SubjectState, GLOBAL_STATE};
use bytes::Bytes;
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
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
    type Error<DE, SE>
        = Error
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type Options = MemorySubscriptionOptions;

    type Subject = MemorySubject<T, D, S>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        subject_string: String,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error<D, S>> {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<SubjectState<T>>() {
            state.put(SubjectState::<T>::default());
        }
        let subject_state = state.borrow::<SubjectState<T>>();
        let mut subjects = subject_state.subjects.lock().await;
        let sender = subjects
            .entry(subject_string.clone())
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
                            // TODO: Handle errors
                            let message = message.clone();
                            let _ = handler.handle(message.clone()).await;
                        }
                    }
                }
            }
        });

        Ok(subscriber)
    }
}
