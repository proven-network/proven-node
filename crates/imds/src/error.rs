use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Base64 decoding error.
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),

    /// HTTP client error.
    #[error(transparent)]
    Http(#[from] httpclient::Error<http::response::Response<httpclient::InMemoryBody>>),

    /// JSON decode error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// No PEM data for current region.
    #[error("No pem data for current region: {0}")]
    NoPemForRegion(String),

    /// PEM error.
    #[error(transparent)]
    Pem(#[from] pem::PemError),

    /// Signature verification failure.
    #[error("Signature verification failed: {0}")]
    SignatureVerification(String),

    /// X.509 error.
    #[error(transparent)]
    X509(#[from] x509_parser::nom::Err<x509_parser::error::X509Error>),
}
