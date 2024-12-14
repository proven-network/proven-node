use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;

/// A marker trait to indicate that the responder has been consumed.
pub trait UsedSubscriptionResponder: Debug + Send + Sync + 'static {}

/// An error type for responders.
pub trait SubscriptionResponderError: Debug + Error + Send + Sync + 'static {}

/// A trait representing a responder to a request.
#[async_trait]
pub trait SubscriptionResponder<R, D, S>
where
    Self: Debug + Send + Sync + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the responder.
    type Error: SubscriptionResponderError;

    /// Corrosponding used type.
    type UsedResponder: UsedSubscriptionResponder;

    /// Does not reply to the request.
    async fn no_reply(self) -> Self::UsedResponder;

    /// Replies to the request with the given response.
    async fn reply(self, response: R) -> Self::UsedResponder;

    /// Streams the given response.
    async fn stream<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Sync + Unpin + 'static;
}
