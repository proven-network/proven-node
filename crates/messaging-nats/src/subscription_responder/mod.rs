mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;

use async_nats::Client as NatsClient;
use async_nats::HeaderMap;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::subscription_responder::{SubscriptionResponder, UsedSubscriptionResponder};
use uuid::Uuid;

/// A used responder for a NATS subscription.
#[derive(Debug)]
pub struct NatsUsedSubscriptionResponder;
impl UsedSubscriptionResponder for NatsUsedSubscriptionResponder {}

/// A responder for a NATS service.
#[derive(Clone, Debug)]
pub struct NatsSubscriptionResponder<R, RD, RS>
where
    Self: Send + Sync + 'static,
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
    nats_client: NatsClient,
    reply_stream_name: String,
    request_id: String,
    _marker: PhantomData<R>,
}

impl<R, RD, RS> NatsSubscriptionResponder<R, RD, RS>
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
    /// Creates a new NATS service responder.
    #[must_use]
    pub const fn new(
        nats_client: NatsClient,
        reply_stream_name: String,
        request_id: String,
    ) -> Self {
        Self {
            nats_client,
            reply_stream_name,
            request_id,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<R, D, S> SubscriptionResponder<R, D, S> for NatsSubscriptionResponder<R, D, S>
where
    R: Clone
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

    type UsedResponder = NatsUsedSubscriptionResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        NatsUsedSubscriptionResponder
    }

    async fn reply(self, response: R) -> Self::UsedResponder {
        let result_bytes: Bytes = response.try_into().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("request-id", self.request_id.clone());

        self.nats_client
            .publish_with_headers(self.reply_stream_name, headers, result_bytes)
            .await
            .unwrap();

        NatsUsedSubscriptionResponder
    }

    async fn stream<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin,
    {
        let mut peekable_stream = stream.peekable();
        let mut pinned_stream = Pin::new(&mut peekable_stream);

        let stream_uuid = Uuid::new_v4();
        let mut next_headers = HeaderMap::new();
        next_headers.insert("request-id", self.request_id.clone());
        next_headers.insert("stream", format!("next:{stream_uuid}"));

        match (pinned_stream.next().await, pinned_stream.peek().await) {
            (Some(payload), Some(_)) => {
                let bytes: Bytes = payload.try_into().unwrap();
                self.nats_client
                    .publish_with_headers(self.reply_stream_name, next_headers, bytes)
                    .await
                    .unwrap();
            }
            (Some(payload), None) => {
                let mut end_headers = HeaderMap::new();
                end_headers.insert("request-id", self.request_id);
                end_headers.insert("stream", format!("end:{stream_uuid}"));

                let bytes: Bytes = payload.try_into().unwrap();
                self.nats_client
                    .publish_with_headers(self.reply_stream_name, end_headers, bytes)
                    .await
                    .unwrap();
            }
            (None, Some(_) | None) => unreachable!(),
        }

        NatsUsedSubscriptionResponder
    }
}
