//! Error types for VSOCK logger

use thiserror::Error;

/// Errors that can occur in the VSOCK logger
#[derive(Debug, Error)]
pub enum Error {
    /// Failed to connect to VSOCK endpoint
    #[error("VSOCK connection failed: {0}")]
    ConnectionFailed(String),

    /// Failed to serialize log data
    #[error("Serialization failed: {0}")]
    SerializationFailed(String),

    /// Failed to send log data
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// RPC error
    #[error("RPC error: {0}")]
    Rpc(#[from] proven_vsock_rpc::Error),
}

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
