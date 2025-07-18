mod error;

pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use proven_messaging::subscription_responder::{SubscriptionResponder, UsedSubscriptionResponder};
use uuid::Uuid;

/// A used responder for a memory subscription.
#[derive(Debug)]
pub struct MemoryUsedSubscriptionResponder;
impl UsedSubscriptionResponder for MemoryUsedSubscriptionResponder {}

/// A responder for a memory subscription.
#[derive(Clone, Debug)]
pub struct MemorySubscriptionResponder<R, D, S>
where
    Self: Send + Sync + 'static,
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
    _reply_subject_name: String,
    _request_id: String,
    _marker: PhantomData<R>,
}

impl<R, D, S> MemorySubscriptionResponder<R, D, S>
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
    /// Creates a new memory service responder.
    #[must_use]
    pub const fn new(reply_subject_name: String, request_id: String) -> Self {
        Self {
            _reply_subject_name: reply_subject_name,
            _request_id: request_id,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<R, D, S> SubscriptionResponder<R, D, S> for MemorySubscriptionResponder<R, D, S>
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

    type UsedResponder = MemoryUsedSubscriptionResponder;

    async fn no_reply(self) -> Self::UsedResponder {
        MemoryUsedSubscriptionResponder
    }

    async fn reply(self, _response: R) -> Self::UsedResponder {
        // TODO

        MemoryUsedSubscriptionResponder
    }

    async fn stream<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin + 'static,
    {
        tokio::spawn(async move {
            let mut peekable_stream = stream.peekable();
            let mut pinned_stream = Pin::new(&mut peekable_stream);

            let _stream_uuid = Uuid::new_v4();

            match (pinned_stream.next().await, pinned_stream.peek().await) {
                (Some(_payload), Some(_)) => {
                    // TODO
                }
                (Some(_payload), None) => {
                    // TODO
                }
                (None, Some(_) | None) => unreachable!(),
            }
        });

        MemoryUsedSubscriptionResponder
    }
}
