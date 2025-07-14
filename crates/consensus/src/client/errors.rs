//! Client-specific error types
//!
//! This module defines error types specific to the client API,
//! providing more context for client operations.

use thiserror::Error;

/// Errors specific to client operations
#[derive(Error, Debug)]
pub enum ClientError {
    /// Stream not found
    #[error("Stream '{0}' not found")]
    StreamNotFound(String),

    /// Stream already exists
    #[error("Stream '{0}' already exists")]
    StreamAlreadyExists(String),

    /// Consumer error
    #[error("Consumer error: {0}")]
    ConsumerError(String),

    /// Serialization error
    #[error("Failed to serialize message")]
    SerializationError(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Deserialization error
    #[error("Failed to deserialize message")]
    DeserializationError(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Invalid sequence number
    #[error("Invalid sequence number {0} for stream '{1}'")]
    InvalidSequence(u64, String),

    /// Stream is closed
    #[error("Stream '{0}' is closed")]
    StreamClosed(String),

    /// Timeout waiting for response
    #[error("Operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// PubSub error
    #[error("PubSub error: {0}")]
    PubSub(#[from] crate::pubsub::PubSubError),

    /// Underlying consensus error
    #[error("Consensus error: {0}")]
    Consensus(#[from] crate::Error),

    /// Generic internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type for client operations
pub type ClientResult<T> = Result<T, ClientError>;

impl ClientError {
    /// Create a serialization error from any error type
    pub fn serialization<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::SerializationError(Box::new(err))
    }

    /// Create a deserialization error from any error type
    pub fn deserialization<E: std::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::DeserializationError(Box::new(err))
    }

    /// Create an internal error from any error type
    pub fn internal<E: std::error::Error>(err: E) -> Self {
        Self::Internal(err.to_string())
    }
}
