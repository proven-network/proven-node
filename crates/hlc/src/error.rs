//! Error types for HLC operations.

use thiserror::Error;

/// Errors that can occur with HLC operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Clock has drifted too far from expected bounds
    #[error("Clock drift exceeded maximum: {0}ms")]
    ClockDrift(f64),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Node ID mismatch or invalid
    #[error("Invalid node ID")]
    InvalidNodeId,

    /// Time went backwards beyond acceptable threshold
    #[error("Time regression detected: {0}ms")]
    TimeRegression(i64),

    /// Provider is unhealthy
    #[error("HLC provider unhealthy: {0}")]
    Unhealthy(String),
}

/// Result type for HLC operations.
pub type Result<T> = std::result::Result<T, Error>;
