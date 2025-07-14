//! Error types for the network crate

use proven_transport::error::TransportError;
use proven_verification::VerificationError;
use thiserror::Error;

/// Result type alias for network operations
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Network error types
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Transport layer error
    #[error("Transport error: {0}")]
    Transport(#[from] TransportError),

    /// Verification error
    #[error("Verification error: {0}")]
    Verification(#[from] VerificationError),

    /// Message serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Invalid message format
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Handler not found for message type
    #[error("No handler registered for message type: {0}")]
    NoHandler(String),

    /// Request timeout
    #[error("Request timeout: {0}")]
    Timeout(String),

    /// Channel closed error
    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Peer not found
    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    /// Network not started
    #[error("Network manager not started")]
    NotStarted,

    /// Other errors
    #[error("{0}")]
    Other(String),

    /// Boxed error for handler errors
    #[error(transparent)]
    Handler(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<ciborium::de::Error<std::io::Error>> for NetworkError {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        NetworkError::Serialization(format!("CBOR deserialization error: {err}"))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for NetworkError {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        NetworkError::Serialization(format!("CBOR serialization error: {err}"))
    }
}
