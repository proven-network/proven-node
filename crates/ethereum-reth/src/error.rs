//! Error types for Reth integration.

use std::io;

use thiserror::Error;

/// Result type for Reth operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when working with Reth.
#[derive(Error, Debug)]
pub enum Error {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(String),

    /// Process error
    #[error("Process error: {0}")]
    Process(String),

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    HttpRequest(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(String),

    /// Timeout error
    #[error("Timeout error: {0}")]
    Timeout(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::HttpRequest(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err.to_string())
    }
}
