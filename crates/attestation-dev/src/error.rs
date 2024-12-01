use proven_attestation::AttestorError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("CBOR error")]
    Cbor,

    #[error(transparent)]
    Cose(#[from] coset::CoseError),
}

impl AttestorError for Error {}
