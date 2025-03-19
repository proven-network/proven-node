//! Error types for the network implementation.

use thiserror::Error;

/// Error type for the network implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Error from the governance implementation.
    #[error("governance error: {0}")]
    Governance(String),

    /// Error when a node is not found in the topology.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// Error related to private key operations.
    #[error("private key error: {0}")]
    PrivateKey(String),
}
