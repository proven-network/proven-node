//! Subscription management in the engine messaging system.
//!
//! Subscriptions in the engine messaging system.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_logger::info;
use tokio::sync::RwLock;
use tokio::time::Duration;

use proven_messaging::subscription::{Subscription, SubscriptionOptions};
use proven_messaging::subscription_handler::SubscriptionHandler;

use crate::error::MessagingEngineError;

/// Internal shared subscription data
#[derive(Debug)]
struct EngineMessagingSubscriptionInner {
    /// Subject name being subscribed to
    subject_name: String,
    /// Unique subscription identifier
    subscription_id: String,
}

/// A subscription to an engine-backed subject pattern
pub struct EngineMessagingSubscription<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Shared subscription data
    inner: Arc<RwLock<EngineMessagingSubscriptionInner>>,
    /// The handler for this subscription
    handler: Arc<RwLock<X>>,
    /// Type markers
    _marker: PhantomData<(Tr, G, St, X, T, D, S)>,
}

impl<Tr, G, St, X, T, D, S> Debug for EngineMessagingSubscription<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineMessagingSubscription")
            .field("inner", &"<EngineMessagingSubscriptionInner>")
            .field("handler", &"<Handler>")
            .finish()
    }
}

impl<Tr, G, St, X, T, D, S> Clone for EngineMessagingSubscription<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
    X: SubscriptionHandler<T, D, S> + Clone,
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
            inner: self.inner.clone(),
            handler: self.handler.clone(),
            _marker: PhantomData,
        }
    }
}

/// Options for creating an engine subscription
#[derive(Debug, Clone)]
pub struct EngineMessagingSubscriptionOptions {
    /// Maximum number of messages to buffer before blocking
    pub buffer_size: usize,
    /// Timeout for message processing
    pub message_timeout: Duration,
    /// Whether to start from the beginning or only new messages
    pub start_from_beginning: bool,
}

impl Default for EngineMessagingSubscriptionOptions {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            message_timeout: Duration::from_secs(30),
            start_from_beginning: false,
        }
    }
}

impl SubscriptionOptions for EngineMessagingSubscriptionOptions {}

#[async_trait]
impl<Tr, G, St, X, T, D, S> Subscription<X, T, D, S>
    for EngineMessagingSubscription<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    type Error = MessagingEngineError;
    type Options = EngineMessagingSubscriptionOptions;
    type Subject = crate::subject::EngineMessagingSubject<Tr, G, St, T, D, S>;

    async fn new(
        subject: Self::Subject,
        _options: Self::Options,
        handler: X,
    ) -> Result<Self, Self::Error> {
        info!(
            "Creating engine subscription for subject: {}",
            subject.name()
        );

        // Since pubsub isn't exposed via engine client yet, we'll create a placeholder
        // TODO: Implement actual subscription when pubsub is exposed via engine client

        // Create a unique subscription ID
        let subscription_id = format!("engine_sub_{}", uuid::Uuid::new_v4());

        // Get the subject name
        let subject_name: String = subject.into();

        // Create inner subscription data
        let inner = Arc::new(RwLock::new(EngineMessagingSubscriptionInner {
            subject_name,
            subscription_id,
        }));

        Ok(Self {
            inner,
            handler: Arc::new(RwLock::new(handler)),
            _marker: PhantomData,
        })
    }
}

impl<Tr, G, St, X, T, D, S> EngineMessagingSubscription<Tr, G, St, X, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Get the subject name for this subscription
    pub async fn subject_name(&self) -> String {
        self.inner.read().await.subject_name.clone()
    }

    /// Get the subscription ID
    pub async fn subscription_id(&self) -> String {
        self.inner.read().await.subscription_id.clone()
    }
}

// TODO: Add tests for engine-based subscriptions when pubsub is exposed via engine client
