use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// PKCS#1 decoding error.
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    /// IO error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// KMS error.
    #[error(transparent)]
    Kms(#[from] aws_sdk_kms::Error),

    /// KMS response missing ciphertext blob.
    #[error("KMS response missing ciphertext blob")]
    MissingCiphertext,

    /// NSM error.
    #[error(transparent)]
    Nsm(#[from] proven_attestation_nsm::Error),

    /// RSA error.
    #[error(transparent)]
    Rsa(#[from] rsa::errors::Error),

    /// SPKI error.
    #[error(transparent)]
    Spki(#[from] rsa::pkcs8::spki::Error),
}
