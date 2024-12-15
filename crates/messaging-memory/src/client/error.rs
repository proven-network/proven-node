use std::fmt::Debug;

use thiserror::Error;

use proven_messaging::client::ClientError;

/// Errors that can occur in a client.
#[derive(Debug, Error)]
pub enum Error {
    /// No response.
    #[error("no response")]
    NoResponse,

    /// No service.
    #[error("no service")]
    NoService,

    /// Error sending.
    #[error("send error")]
    Send,
}

impl ClientError for Error {}
