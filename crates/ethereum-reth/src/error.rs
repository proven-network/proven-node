//! Error types for Reth integration.

use thiserror::Error;

/// Result type for Reth operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when working with Reth.
#[derive(Error, Debug)]
pub enum Error {
    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Error from the isolation crate.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    HttpRequest(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(String),
}
