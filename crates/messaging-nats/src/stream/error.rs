use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Create stream error.
    #[error("failed to create stream: {0}")]
    CreateStream(async_nats::jetstream::context::CreateStreamErrorKind),

    /// Delete error.
    #[error("failed to delete message: {0}")]
    Delete(async_nats::jetstream::stream::DeleteMessageErrorKind),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// Direct get error.
    #[error("failed to get message: {0}")]
    DirectGet(async_nats::jetstream::stream::DirectGetErrorKind),

    /// Stream info error.
    #[error("failed to get stream info: {0}")]
    Info(async_nats::jetstream::context::RequestErrorKind),

    /// Publish error.
    #[error("failed to publish: {0}")]
    Publish(async_nats::jetstream::context::PublishErrorKind),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialize(String),

    /// An error occured while starting a service.
    #[error(transparent)]
    Service(#[from] crate::service::Error),

    /// An error occured while subscribing to a subject.
    #[error("failed to subscribe to subject: {0}")]
    Subject(String),
}

impl StreamError for Error {}
