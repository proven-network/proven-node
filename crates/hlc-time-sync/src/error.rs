//! Error types for the atomic HLC implementation.

use thiserror::Error;

/// Errors that can occur in the atomic HLC implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Chrony command error
    #[error("Chrony error: {0}")]
    Chrony(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// HLC error from core crate
    #[error("HLC error: {0}")]
    Hlc(#[from] proven_hlc::Error),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// NTS not synchronized
    #[error("NTS not synchronized")]
    NtsNotSynchronized,

    /// NTS time validation failed
    #[error("NTS validation failed: drift {0}ms exceeds threshold {1}ms")]
    NtsValidationFailed(f64, f64),

    /// RPC communication error
    #[error("RPC error: {0}")]
    Rpc(#[from] proven_vsock_rpc::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// System time error
    #[error("System time error: {0}")]
    SystemTime(String),
}

/// Result type for atomic HLC operations.
pub type Result<T> = std::result::Result<T, Error>;

// Convert our Error to HLCError
impl From<Error> for proven_hlc::Error {
    fn from(e: Error) -> Self {
        Self::Internal(e.to_string())
    }
}
