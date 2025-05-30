use crate::service_handler::ServiceHandler;
use crate::service_responder::{ServiceResponder, UsedServiceResponder};
use crate::stream::InitializedStream;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
/// Marker trait for service errors
pub trait ServiceError: Error + Send + Sync + 'static {}

/// Marker trait for service options
pub trait ServiceOptions: Clone + Debug + Send + Sync + 'static {}

/// A trait representing a stateful view of a stream which can handle requests.
#[async_trait]
pub trait Service<X, T, D, S>
where
    Self: Bootable + Clone + Debug + Send + Sync + 'static,
    X: ServiceHandler<T, D, S>,
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
    /// The error type for the service.
    type Error: ServiceError;

    /// The options for the service.
    type Options: ServiceOptions;

    /// The responder type for the subscriber.
    type Responder: ServiceResponder<
            T,
            D,
            S,
            X::ResponseType,
            X::ResponseDeserializationError,
            X::ResponseSerializationError,
        >;

    /// The response consumer type for the subscriber.
    type UsedResponder: UsedServiceResponder;

    /// The stream type for the service.
    type StreamType: InitializedStream<T, D, S>;

    /// Creates a new service.
    async fn new(
        name: String,
        stream: Self::StreamType,
        options: Self::Options,
        handler: X,
    ) -> Result<Self, <Self as Service<X, T, D, S>>::Error>;

    /// Gets the last sequence number processed by the service.
    async fn last_seq(&self) -> Result<u64, <Self as Service<X, T, D, S>>::Error>;

    /// Gets the stream for the service.
    fn stream(&self) -> Self::StreamType;
}
