//! Transport error types

use thiserror::Error;

/// Transport-related errors
#[derive(Debug, Error)]
pub enum TransportError {
    /// Connection failed
    #[error("Connection failed to {peer}: {reason}")]
    ConnectionFailed { peer: String, reason: String },

    /// Timeout occurred
    #[error("Timeout: {operation}")]
    Timeout { operation: String },

    /// Invalid message
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Authentication failed
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Transport error
    #[error("Transport error: {0}")]
    Transport(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Other error
    #[error("{0}")]
    Other(String),
}

impl TransportError {
    /// Create an authentication failed error
    pub fn auth_failed(reason: impl std::fmt::Display) -> Self {
        Self::AuthenticationFailed(reason.to_string())
    }

    /// Create a connection failed error
    pub fn connection_failed(peer: impl std::fmt::Display, reason: impl std::fmt::Display) -> Self {
        Self::ConnectionFailed {
            peer: peer.to_string(),
            reason: reason.to_string(),
        }
    }

    /// Create an invalid message error
    pub fn invalid_message(msg: impl std::fmt::Display) -> Self {
        Self::InvalidMessage(msg.to_string())
    }

    /// Create a timeout error
    pub fn timeout(operation: impl std::fmt::Display) -> Self {
        Self::Timeout {
            operation: operation.to_string(),
        }
    }

    /// Create a transport error
    pub fn transport(error: impl std::fmt::Display) -> Self {
        Self::Transport(error.to_string())
    }
}
