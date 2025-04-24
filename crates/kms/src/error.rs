use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// PKCS#1 decoding error.
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    /// Attestation error.
    #[error("attestation error: {0}")]
    Attestation(String),

    /// IO error.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// KMS error.
    #[error(transparent)]
    Kms(#[from] aws_sdk_kms::Error),

    /// KMS response missing ciphertext blob.
    #[error("KMS response missing ciphertext blob")]
    MissingCiphertext,

    /// RSA error.
    #[error(transparent)]
    Rsa(#[from] rsa::errors::Error),

    /// SPKI error.
    #[error(transparent)]
    Spki(#[from] rsa::pkcs8::spki::Error),
}
