use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Deserialization error.
    #[error(transparent)]
    Deserialize(#[from] ciborium::de::Error<std::io::Error>),

    /// Direct get error.
    #[error("Failed to get message: {0}")]
    DirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    /// Stream info error.
    #[error("Failed to get stream info: {0}")]
    Info(async_nats::jetstream::context::RequestErrorKind),

    /// Publish error.
    #[error("Failed to publish: {0}")]
    Publish(async_nats::jetstream::context::PublishErrorKind),

    /// Serialization error.
    #[error(transparent)]
    Serialize(#[from] ciborium::ser::Error<std::io::Error>),

    /// An error occured while subscribing to a subject.
    #[error(transparent)]
    Subject(#[from] crate::subject::Error),
}

impl StreamError for Error {}
