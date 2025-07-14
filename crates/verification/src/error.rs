//! Error types for the verification crate

use thiserror::Error;

/// Verification-related errors
#[derive(Debug, Error)]
pub enum VerificationError {
    /// Invalid message format or content
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Attestation verification failed
    #[error("Attestation error: {0}")]
    Attestation(String),

    /// Governance operation failed
    #[error("Governance error: {0}")]
    Governance(String),

    /// Signature verification failed
    #[error("Signature verification failed: {0}")]
    SignatureVerification(String),

    /// Connection verification failed
    #[error("Connection verification failed: {0}")]
    ConnectionVerification(String),

    /// Timeout during verification
    #[error("Verification timeout: {0}")]
    Timeout(String),

    /// Other errors
    #[error("{0}")]
    Other(String),
}

/// Result type for verification operations
pub type VerificationResult<T> = Result<T, VerificationError>;
