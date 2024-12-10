mod error;

use crate::subject::MemorySubject;
use crate::{SubjectState, GLOBAL_STATE};
use bytes::Bytes;
pub use error::Error;
use proven_messaging::subject::Subject;
use proven_messaging::Message;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemorySubscriptionOptions;
impl SubscriptionOptions for MemorySubscriptionOptions {}

/// A in-memory subscriber.
#[derive(Debug)]
pub struct MemorySubscription<P, X, T, D, S>
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
    cancel_token: CancellationToken,
    handler: X,
    last_message: Arc<Mutex<Option<Message<T>>>>,
    _marker: PhantomData<P>,
}

impl<P, X, T, D, S> Clone for MemorySubscription<P, X, T, D, S>
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
            cancel_token: self.cancel_token.clone(),
            handler: self.handler.clone(),
            last_message: self.last_message.clone(),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, X, T, D, S> Subscription<P, X, T, D, S> for MemorySubscription<P, X, T, D, S>
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
    type Error = Error;

    type Options = MemorySubscriptionOptions;

    type SubjectType = MemorySubject<T, D, S>;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        subject_string: String,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
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

        let subscriber = Self {
            cancel_token: CancellationToken::new(),
            handler,
            last_message: Arc::new(Mutex::new(None)),
            _marker: PhantomData,
        };

        let subscriber_clone = subscriber.clone();
        let cancel_token = subscriber.cancel_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel_token.cancelled() => {
                        break;
                    }
                    message = receiver.recv() => {
                        if let Ok(message) = message {
                            // TODO: Handle errors
                            let message = message.clone();
                            let _ = subscriber_clone.handler().handle(message.clone()).await;
                            subscriber_clone.last_message.lock().await.replace(message);
                        }
                    }
                }
            }
        });

        Ok(subscriber)
    }

    async fn cancel(self) -> Result<(), Self::Error> {
        self.cancel_token.cancel();
        Ok(())
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_message(&self) -> Option<Message<T>> {
        self.last_message.lock().await.clone()
    }
}
