mod error;

use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use proven_messaging::{Handler, Subscriber};
use tokio::sync::{broadcast, Mutex};

/// A in-memory subscriber.
#[derive(Clone, Debug, Default)]
pub struct InMemorySubscriber<T, X> {
    handler: X,
    last_message: Arc<Mutex<Option<T>>>,
}

#[async_trait]
impl<T, X> Subscriber<T, X> for InMemorySubscriber<T, X>
where
    T: Clone + Debug + Send + Sync + 'static,
    X: Handler<T>,
{
    type Error = Error;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(subject_string: String, handler: X) -> Result<Self, Self::Error> {
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

    fn handler(&self) -> X {
        self.handler.clone()
    }

    async fn last_message(&self) -> Option<T> {
        self.last_message.lock().await.clone()
    }
}
