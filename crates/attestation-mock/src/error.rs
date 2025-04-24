use nsm_nitro_enclave_utils::verify::{ErrorKind, VerifyError};
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

    /// Error verifying attestation.
    #[error("error verifying attestation: {0}")]
    Verification(&'static str),
}

impl AttestorError for Error {}

impl From<VerifyError> for Error {
    fn from(value: VerifyError) -> Self {
        match value.kind() {
            ErrorKind::AttestationDoc => Self::Verification("attestation doc"),
            ErrorKind::Cose => Self::Verification("cose"),
            ErrorKind::EndCertificate => Self::Verification("end certificate"),
            ErrorKind::RootCertificate => Self::Verification("root certificate"),
            ErrorKind::Verification => Self::Verification("verification"),
        }
    }
}
