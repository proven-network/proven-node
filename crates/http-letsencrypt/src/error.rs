use proven_bootable::BootableError;
use proven_http::HttpServerError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The server has already been started.
    #[error("the server has already been started")]
    AlreadyStarted,
}

impl BootableError for Error {}
impl HttpServerError for Error {}
