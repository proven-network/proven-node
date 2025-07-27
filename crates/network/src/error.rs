//! Error types for the streaming network layer

use proven_topology::NodeId;
use std::time::Duration;
use thiserror::Error;

/// Network operation errors
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Transport layer error
    #[error("Transport error: {0}")]
    Transport(String),

    /// Connection failed
    #[error("Failed to connect to {node}: {reason}")]
    ConnectionFailed { node: Box<NodeId>, reason: String },

    /// Stream error
    #[error("Stream error: {0}")]
    Stream(#[from] StreamError),

    /// Service not found
    #[error("Service '{service}' not found")]
    ServiceNotFound { service: String },

    /// Handler already registered
    #[error("Handler already registered for service '{service}'")]
    HandlerAlreadyRegistered { service: String },

    /// No handler registered
    #[error("No handler registered for service '{service}'")]
    NoHandler { service: String },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Timeout
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    /// Channel closed
    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    /// Invalid message
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Peer not found
    #[error("Peer not found: {0}")]
    PeerNotFound(Box<NodeId>),

    /// Invalid address
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// Service error
    #[error("Service error: {0}")]
    ServiceError(String),

    /// Connection not verified
    #[error("Connection not verified")]
    ConnectionNotVerified,

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Stream-specific errors
#[derive(Debug, Error)]
pub enum StreamError {
    /// Stream closed
    #[error("Stream {id} closed")]
    Closed { id: uuid::Uuid },

    /// Flow control error
    #[error("Flow control error: {0}")]
    FlowControl(String),

    /// Would block (for try_send operations)
    #[error("Operation would block")]
    WouldBlock,

    /// Stream limit exceeded
    #[error("Stream limit exceeded: {0}")]
    LimitExceeded(String),

    /// Protocol error
    #[error("Stream protocol error: {0}")]
    Protocol(String),

    /// Timeout error
    #[error("Stream operation timed out")]
    Timeout,
}

/// Result type alias
pub type NetworkResult<T> = Result<T, NetworkError>;
