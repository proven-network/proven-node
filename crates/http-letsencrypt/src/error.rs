use proven_http::HttpServerError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The server has already been started.
    #[error("the server has already been started")]
    AlreadyStarted,

    /// Failed to store certificate.
    #[error("certificate store error: {0}")]
    CertStore(String),
}

impl HttpServerError for Error {}
