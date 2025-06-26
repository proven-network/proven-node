use proven_http::HttpServerError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The server has already been started.
    #[error("the server has already been started")]
    AlreadyStarted,
}

impl HttpServerError for Error {}
