use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to store certificate.
    #[error("certificate store error: {0}")]
    CertStore(String),
}
