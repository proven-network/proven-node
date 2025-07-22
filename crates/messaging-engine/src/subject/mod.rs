//! Subjects in the engine messaging system.

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;
use crate::subscription::{EngineMessagingSubscription, EngineMessagingSubscriptionOptions};

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_logger::debug;
use proven_messaging::subject::Subject;

/// An engine messaging subject.
pub struct EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Subject name
    name: String,

    _marker: PhantomData<(Tr, G, St, T, D, S)>,
}

impl<Tr, G, St, T, D, S> Debug for EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
        f.debug_struct("EngineMessagingSubject")
            .field("name", &self.name)
            .finish()
    }
}

impl<Tr, G, St, T, D, S> Clone for EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Tr, G, St, T, D, S> EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    /// Creates a new engine messaging subject.
    #[must_use]
    pub const fn new(name: String) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }

    /// Get the subject name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl<Tr, G, St, T, D, S> Subject<T, D, S> for EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    type StreamType = InitializedEngineStream<Tr, G, St, T, D, S>;
    type SubscriptionType<X>
        = EngineMessagingSubscription<Tr, G, St, X, T, D, S>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>,
    {
        debug!("Creating subscription for subject '{}'", self.name);

        let options = EngineMessagingSubscriptionOptions::default();

        <EngineMessagingSubscription<Tr, G, St, X, T, D, S> as proven_messaging::subscription::Subscription<
            X,
            T,
            D,
            S,
        >>::new(self.clone(), options, handler)
        .await
    }

    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <<Self as Subject<T, D, S>>::StreamType as proven_messaging::stream::InitializedStream<T, D, S>>::Options,
    ) -> Result<
        Self::StreamType,
        <<Self as Subject<T, D, S>>::StreamType as proven_messaging::stream::InitializedStream<
            T,
            D,
            S,
        >>::Error,
    >
    where
        K: Into<String> + Send,
    {
        let stream_name = stream_name.into();
        debug!(
            "Creating stream '{}' from subject '{}'",
            stream_name, self.name
        );

        <InitializedEngineStream<Tr, G, St, T, D, S> as proven_messaging::stream::InitializedStream<
            T,
            D,
            S,
        >>::new(stream_name, options)
        .await
    }
}

impl<Tr, G, St, T, D, S> From<String> for EngineMessagingSubject<Tr, G, St, T, D, S>
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl<Tr, G, St, T, D, S> From<EngineMessagingSubject<Tr, G, St, T, D, S>> for String
where
    Tr: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    St: proven_storage::StorageAdaptor + 'static,
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
    fn from(val: EngineMessagingSubject<Tr, G, St, T, D, S>) -> Self {
        val.name
    }
}
