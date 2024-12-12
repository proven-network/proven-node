use std::fmt::Debug;

use thiserror::Error;

use proven_messaging::client::ClientError;

/// Errors that can occur in a client.
#[derive(Debug, Error)]
pub enum Error {
    /// Handler error.
    #[error("Handler error")]
    Something(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// No response.
    #[error("No response")]
    NoResponse,

    /// Task failure.
    #[error("Task failure")]
    Task,
}

impl ClientError for Error {}
