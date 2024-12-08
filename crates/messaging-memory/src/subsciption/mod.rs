mod error;

use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{broadcast, Mutex};

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct InMemorySubscriberOptions;
impl SubscriptionOptions for InMemorySubscriberOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug, Default)]
pub struct InMemorySubscriber<X, T = Bytes> {
    handler: X,
    last_message: Arc<Mutex<Option<T>>>,
}

#[async_trait]
impl<X, T> Subscription<X, T> for InMemorySubscriber<X, T>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: SubscriptionHandler<T>,
{
    type Error = Error;
    type Options = InMemorySubscriberOptions;

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
            handler,
            last_message: Arc::new(Mutex::new(None)),
        };

        let subscriber_clone = subscriber.clone();
        tokio::spawn(async move {
            while let Ok((message, headers)) = receiver.recv().await {
                let _ = subscriber_clone
                    .handler()
                    .handle(subject_string.clone(), message.clone(), headers)
                    .await;
                subscriber_clone.last_message.lock().await.replace(message);
            }
        });

        Ok(subscriber)
    }

    async fn cancel(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_message(&self) -> Option<T> {
        self.last_message.lock().await.clone()
    }
}
