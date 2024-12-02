use proven_attestation::AttestorError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// CBOR error.
    #[error("CBOR error")]
    Cbor,

    /// COSE error.
    #[error(transparent)]
    Cose(#[from] coset::CoseError),
}

impl AttestorError for Error {}
