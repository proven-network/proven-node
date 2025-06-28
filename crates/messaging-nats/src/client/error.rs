use std::fmt::Debug;

use thiserror::Error;

use proven_messaging::client::ClientError;

/// Errors that can occur in a client.
#[derive(Debug, Error)]
pub enum Error {
    /// Consumer create error.
    #[error("Failed to create consumer: {0}")]
    CreateConsumer(async_nats::jetstream::stream::ConsumerErrorKind),

    /// Create stream error.
    #[error("failed to create stream: {0}")]
    CreateStream(async_nats::jetstream::context::CreateStreamErrorKind),

    /// Deserialization error.
    #[error("deserialization error")]
    Deserialization,

    /// Batch item length.
    #[error("batch item length could not be determined")]
    BatchItemLength,

    /// No response.
    #[error("no response")]
    NoResponse,

    /// Task failure.
    #[error("task failure")]
    Task,
}

impl ClientError for Error {}
