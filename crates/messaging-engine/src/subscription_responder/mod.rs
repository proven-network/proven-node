//! Subscription responders handle subscription responses.

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;
use proven_logger::debug;

use crate::error::MessagingEngineError;
use proven_messaging::subscription_responder::{SubscriptionResponder, UsedSubscriptionResponder};

/// A used responder for an engine subscription.
#[derive(Clone, Debug)]
pub struct EngineMessagingUsedSubscriptionResponder;

impl UsedSubscriptionResponder for EngineMessagingUsedSubscriptionResponder {}

/// A responder for an engine subscription.
#[derive(Clone, Debug)]
pub struct EngineMessagingSubscriptionResponder<R, RD, RS>
where
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Subject name this responder belongs to
    subject_name: String,
    /// Request ID for correlation
    request_id: String,
    /// Type marker
    _marker: PhantomData<(R, RD, RS)>,
}

impl<R, RD, RS> EngineMessagingSubscriptionResponder<R, RD, RS>
where
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new engine subscription responder.
    #[must_use]
    pub const fn new(subject_name: String, request_id: String) -> Self {
        Self {
            subject_name,
            request_id,
            _marker: PhantomData,
        }
    }

    /// Get the subject name for this responder.
    #[must_use]
    pub fn subject_name(&self) -> &str {
        &self.subject_name
    }
}

#[async_trait]
impl<R, RD, RS> SubscriptionResponder<R, RD, RS> for EngineMessagingSubscriptionResponder<R, RD, RS>
where
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    RD: Debug + Send + StdError + Sync + 'static,
    RS: Debug + Send + StdError + Sync + 'static,
{
    type Error = MessagingEngineError;
    type UsedResponder = EngineMessagingUsedSubscriptionResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        debug!("No reply for subscription request '{}'", self.request_id);
        EngineMessagingUsedSubscriptionResponder
    }

    async fn reply(self, _response: R) -> Self::UsedResponder {
        // TODO: Implement actual reply when pubsub is exposed via engine client
        debug!(
            "Reply for subscription request '{}' (not implemented)",
            self.request_id
        );
        EngineMessagingUsedSubscriptionResponder
    }

    async fn stream<W>(self, _response_stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin,
    {
        // TODO: Implement actual streaming when pubsub is exposed via engine client
        debug!(
            "Stream for subscription request '{}' (not implemented)",
            self.request_id
        );
        EngineMessagingUsedSubscriptionResponder
    }
}
