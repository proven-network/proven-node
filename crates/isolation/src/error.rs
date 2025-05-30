//! Error types for isolation operations.

use std::io;
use std::process::ExitStatus;

use thiserror::Error;

/// Result type for isolation operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during isolation operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Network error
    #[error("Network error: {0}")]
    Network(String),

    /// IO error.
    #[error("io error: {0} - {1}")]
    Io(&'static str, #[source] io::Error),

    /// Failed to set up namespaces
    #[error("Failed to set up namespaces: {0}")]
    Namespace(String),

    /// Error parsing data
    #[error("Parse error: {0}")]
    ParseInt(String),

    /// Process exited with non-zero status
    #[error("Process exited with non-zero status: {0}")]
    ProcessExit(ExitStatus),

    /// Process failed readiness checks
    #[error("Process failed readiness checks: {0}")]
    ReadinessCheck(String),

    /// Failed to spawn a process
    #[error("Failed to spawn process: {0}")]
    SpawnProcess(String),
}
