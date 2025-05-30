use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

use crate::subscription_responder::SubscriptionResponder;

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait SubscriptionHandler<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: Error + Send + Sync + 'static;

    /// The response type for the subscriber.
    type ResponseType: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = Self::ResponseDeserializationError>
        + TryInto<Bytes, Error = Self::ResponseSerializationError>
        + 'static;

    /// Deserialization error for responses.
    type ResponseDeserializationError: Error + Send + Sync + 'static;

    /// Serialization error for responses.
    type ResponseSerializationError: Error + Send + Sync + 'static;

    /// Handles the given data and optionally reply.
    async fn handle<R>(&self, message: T, responder: R) -> Result<R::UsedResponder, Self::Error>
    where
        R: SubscriptionResponder<
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >;
}
