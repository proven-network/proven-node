//! Error types for the CAC RPC implementation.

use thiserror::Error;

/// Result type alias for CAC operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for CAC operations.
#[derive(Debug, Error)]
pub enum Error {
    /// RPC framework error.
    #[error("RPC error: {0}")]
    Rpc(#[from] proven_vsock_rpc::Error),

    /// Invalid response type received.
    #[error("Invalid response type: expected {expected}, got {actual}")]
    InvalidResponseType {
        /// Expected response type.
        expected: String,
        /// Actual response type.
        actual: String,
    },

    /// Command failed to execute.
    #[error("Command failed: {0}")]
    CommandFailed(String),

    /// Generic error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
