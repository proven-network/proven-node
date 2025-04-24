use proven_attestation::AttestorError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Invalid attestation response.
    #[error("Invalid attestation response")]
    InvalidResponse,
}

impl AttestorError for Error {}
