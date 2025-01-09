use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::Stream;

/// A marker trait to indicate that the responder has been consumed.
pub trait UsedServiceResponder: Debug + Send + Sync + 'static {}

/// An error type for responders.
pub trait ServiceResponderError: Debug + Error + Send + Sync + 'static {}

/// A trait representing a responder to a request.
#[async_trait]
pub trait ServiceResponder<T, TD, TS, R, RD, RS>
where
    Self: Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = TD>
        + TryInto<Bytes, Error = TS>
        + 'static,
    R: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = RD>
        + TryInto<Bytes, Error = RS>
        + 'static,
    TD: Debug + Error + Send + Sync + 'static,
    TS: Debug + Error + Send + Sync + 'static,
    RD: Debug + Error + Send + Sync + 'static,
    RS: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the responder.
    type Error: ServiceResponderError;

    /// Corrosponding used type.
    type UsedResponder: UsedServiceResponder;

    /// Does not reply to the request.
    async fn no_reply(self) -> Self::UsedResponder;

    /// Replies to the request with the given response.
    async fn reply(self, response: R) -> Self::UsedResponder;

    /// Replies to the request with the given response and deletes the request.
    async fn reply_and_delete_request(self, response: R) -> Self::UsedResponder;

    /// Streams the given response.
    async fn stream<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin;

    /// Streams the given response and deletes the request.
    async fn stream_and_delete_request<W>(self, stream: W) -> Self::UsedResponder
    where
        W: Stream<Item = R> + Send + Unpin;

    /// Returns the stream sequence number of the request.
    fn stream_sequence(&self) -> u64;
}
