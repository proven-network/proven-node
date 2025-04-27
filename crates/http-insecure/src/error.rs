use proven_bootable::BootableError;
use proven_http::HttpServerError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The server has already been started.
    #[error("The server has already been started")]
    AlreadyStarted,

    /// Failed to bind to address.
    #[error("Failed to bind to address: {0}")]
    Bind(#[from] std::io::Error),
}

impl BootableError for Error {}
impl HttpServerError for Error {}
