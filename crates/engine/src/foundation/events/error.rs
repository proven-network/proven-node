//! Error types for the event system

use std::time::Duration;
use thiserror::Error;

/// Result type alias for event system operations
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the event system
#[derive(Debug, Error)]
pub enum Error {
    /// No handler is registered for the given type
    #[error("No handler registered for {type_name}")]
    NoHandler {
        /// The name of the type that has no handler
        type_name: &'static str,
    },

    /// A request timed out
    #[error("Request timed out after {duration:?}")]
    Timeout {
        /// The duration after which the timeout occurred
        duration: Duration,
    },

    /// The event channel is full and cannot accept more events
    #[error("Channel is full and overflow policy is Error")]
    ChannelFull,

    /// A handler panicked while processing an event
    #[error("Handler panicked while processing event")]
    HandlerPanic,

    /// The event bus is shutting down
    #[error("Event bus is shutting down")]
    Shutdown,

    /// Invalid configuration was provided
    #[error("Invalid configuration: {reason}")]
    InvalidConfig {
        /// Description of what was invalid
        reason: String,
    },

    /// A deadline was exceeded
    #[error("Deadline exceeded")]
    DeadlineExceeded,

    /// A stream ended unexpectedly
    #[error("Stream ended unexpectedly")]
    StreamEnded,

    /// An internal error occurred
    #[error("Internal error: {0}")]
    Internal(String),

    /// Error from flume channel send operation
    #[error(transparent)]
    Channel(#[from] flume::SendError<Box<dyn std::any::Any + Send + Sync>>),

    /// Error from flume channel receive operation
    #[error(transparent)]
    Recv(#[from] flume::RecvError),

    /// Error from flume channel try_recv operation
    #[error(transparent)]
    TryRecv(#[from] flume::TryRecvError),

    /// Error from oneshot channel receive operation
    #[error(transparent)]
    Oneshot(#[from] tokio::sync::oneshot::error::RecvError),
}
