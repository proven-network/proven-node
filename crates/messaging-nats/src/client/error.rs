use std::fmt::Debug;

use thiserror::Error;

use proven_messaging::client::ClientError;

/// Errors that can occur in a client.
#[derive(Debug, Error)]
pub enum Error {
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
