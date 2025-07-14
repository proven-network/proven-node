//! Simple error types for topology operations

use thiserror::Error;

/// Topology-related errors
#[derive(Clone, Debug, Error)]
pub enum TopologyError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Governance error
    #[error("Governance error: {0}")]
    Governance(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}
