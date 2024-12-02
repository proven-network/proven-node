use proven_http::HttpServerError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<SE>
where
    SE: StoreError,
{
    /// The server has already been started.
    #[error("The server has already been started")]
    AlreadyStarted,

    /// Failed to store certificate.
    #[error("Certificate store error: {0}")]
    CertStore(#[from] SE),
}

impl<SE> HttpServerError for Error<SE> where SE: StoreError {}
