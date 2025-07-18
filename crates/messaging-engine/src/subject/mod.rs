//! Subjects in the engine messaging system.

use crate::error::MessagingEngineError;
use crate::stream::InitializedEngineStream;
use crate::subscription::{EngineMessagingSubscription, EngineMessagingSubscriptionOptions};

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subject::Subject;
use tracing::debug;

/// An engine messaging subject.
#[derive(Debug)]
pub struct EngineMessagingSubject<T, D, S>
where
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

    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for EngineMessagingSubject<T, D, S>
where
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

impl<T, D, S> EngineMessagingSubject<T, D, S>
where
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
impl<T, D, S> Subject<T, D, S> for EngineMessagingSubject<T, D, S>
where
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
    type StreamType = InitializedEngineStream<T, D, S>;
    type SubscriptionType<X>
        = EngineMessagingSubscription<X, T, D, S>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: proven_messaging::subscription_handler::SubscriptionHandler<T, D, S>,
    {
        debug!("Creating subscription for subject '{}'", self.name);

        let options = EngineMessagingSubscriptionOptions::default();

        <EngineMessagingSubscription<X, T, D, S> as proven_messaging::subscription::Subscription<
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

        <InitializedEngineStream<T, D, S> as proven_messaging::stream::InitializedStream<
            T,
            D,
            S,
        >>::new(stream_name, options)
        .await
    }
}

impl<T, D, S> From<String> for EngineMessagingSubject<T, D, S>
where
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

impl<T, D, S> From<EngineMessagingSubject<T, D, S>> for String
where
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
    fn from(val: EngineMessagingSubject<T, D, S>) -> Self {
        val.name
    }
}
