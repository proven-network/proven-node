mod error;

use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;
use proven_messaging::Message;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

/// Options for the in-memory subscriber (there are none).
#[derive(Clone, Debug)]
pub struct MemorySubscriptionOptions;
impl SubscriptionOptions for MemorySubscriptionOptions {}

/// A in-memory subscriber.
#[derive(Clone, Debug, Default)]
pub struct MemorySubscription<X, T = Bytes, R = Bytes> {
    cancel_token: CancellationToken,
    handler: X,
    last_message: Arc<Mutex<Option<Message<T>>>>,
    _marker: std::marker::PhantomData<R>,
}

#[async_trait]
impl<X, T, R> Subscription<X> for MemorySubscription<X, T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    X: SubscriptionHandler<Type = T, ResponseType = R> + Clone + Send + Sync + 'static,
    X::Type: Clone + Debug + Send + Sync + 'static,
    X::ResponseType: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error<X::Error>;

    type HandlerError = X::Error;

    type Type = T;

    type ResponseType = R;

    type Options = MemorySubscriptionOptions;

    #[allow(clippy::significant_drop_tightening)]
    async fn new(
        subject_string: String,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Error<X::Error>> {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<SubjectState<Self::Type>>() {
            state.put(SubjectState::<Self::Type>::default());
        }
        let subject_state = state.borrow::<SubjectState<Self::Type>>();
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
            _marker: std::marker::PhantomData,
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

    async fn last_message(&self) -> Option<Message<Self::Type>> {
        self.last_message.lock().await.clone()
    }
}