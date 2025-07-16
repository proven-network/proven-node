//! Error types for the RPC framework.

use std::io;
use std::time::Duration;
use thiserror::Error;

/// Result type alias for RPC operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for RPC operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Connection-related errors.
    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),

    /// Protocol-level errors.
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// Codec errors during serialization/deserialization.
    #[error("Codec error: {0}")]
    Codec(#[from] CodecError),

    /// Handler errors from the server side.
    #[error("Handler error: {0}")]
    Handler(#[from] HandlerError),

    /// Operation timed out.
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    /// Connection pool exhausted.
    #[error("Connection pool exhausted: {0}")]
    PoolExhausted(String),

    /// Generic I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Channel closed unexpectedly.
    #[error("Channel closed")]
    ChannelClosed,
}

/// Connection-specific errors.
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// Failed to establish connection.
    #[error("Failed to connect to {addr}: {source}")]
    ConnectFailed {
        /// The address we tried to connect to.
        #[cfg(target_os = "linux")]
        addr: tokio_vsock::VsockAddr,
        /// The address we tried to connect to.
        #[cfg(not(target_os = "linux"))]
        addr: std::net::SocketAddr,
        /// The underlying error.
        #[source]
        source: io::Error,
    },

    /// Connection closed unexpectedly.
    #[error("Connection closed unexpectedly")]
    Closed,

    /// Health check failed.
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),

    /// Too many in-flight requests.
    #[error("Too many in-flight requests: {current}/{max}")]
    TooManyInFlight {
        /// Current number of in-flight requests.
        current: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// Connection is shutting down.
    #[error("Connection is shutting down")]
    ShuttingDown,
}

/// Protocol-level errors.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Invalid frame received.
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),

    /// Frame too large.
    #[error("Frame size {size} exceeds maximum {max}")]
    FrameTooLarge {
        /// Size of the frame.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },

    /// Unexpected message type.
    #[error("Unexpected message type: expected {expected}, got {actual}")]
    UnexpectedMessageType {
        /// Expected message type.
        expected: String,
        /// Actual message type received.
        actual: String,
    },

    /// Invalid message ID.
    #[error("Invalid message ID: {0}")]
    InvalidMessageId(String),

    /// Checksum mismatch.
    #[error("Checksum mismatch: expected {expected:08x}, got {actual:08x}")]
    ChecksumMismatch {
        /// Expected checksum.
        expected: u32,
        /// Actual checksum.
        actual: u32,
    },
}

/// Codec-related errors.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Serialization failed.
    #[error("Failed to serialize: {0}")]
    SerializationFailed(String),

    /// Deserialization failed.
    #[error("Failed to deserialize: {0}")]
    DeserializationFailed(String),

    /// Unsupported codec type.
    #[error("Unsupported codec: {0}")]
    UnsupportedCodec(String),
}

/// Handler errors from server-side processing.
#[derive(Debug, Error)]
pub enum HandlerError {
    /// Handler not found for message.
    #[error("No handler registered for message: {0}")]
    NotFound(String),

    /// Handler panicked.
    #[error("Handler panicked: {0}")]
    Panicked(String),

    /// Handler returned an error.
    #[error("Handler error: {0}")]
    Internal(String),
}

impl From<bincode::Error> for CodecError {
    fn from(err: bincode::Error) -> Self {
        Self::SerializationFailed(err.to_string())
    }
}

impl From<ciborium::de::Error<io::Error>> for CodecError {
    fn from(err: ciborium::de::Error<io::Error>) -> Self {
        Self::DeserializationFailed(err.to_string())
    }
}

impl From<ciborium::ser::Error<io::Error>> for CodecError {
    fn from(err: ciborium::ser::Error<io::Error>) -> Self {
        Self::SerializationFailed(err.to_string())
    }
}
