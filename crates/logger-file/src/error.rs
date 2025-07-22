//! Error types for file-based logging

use std::io;
use std::path::PathBuf;

/// Result type for file logger operations
pub type Result<T> = std::result::Result<T, Error>;

/// Error types that can occur during file logging
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Failed to create log directory
    #[error("Failed to create log directory at {path}: {source}")]
    CreateDirectory {
        /// The path that failed to be created
        path: PathBuf,
        /// The underlying error
        source: io::Error,
    },

    /// Failed to rotate log file
    #[error("Failed to rotate log file: {0}")]
    Rotation(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Channel send error
    #[error("Failed to send log message: channel closed")]
    ChannelClosed,

    /// Serialization error
    #[error("Failed to serialize log: {0}")]
    Serialization(#[from] serde_json::Error),
}
