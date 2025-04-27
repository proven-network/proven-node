use proven_bootable::BootableError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Client error.
    #[error("client error: {0}")]
    Client(String),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Service error.
    #[error("service error: {0}")]
    Service(String),
}

impl BootableError for Error {}
