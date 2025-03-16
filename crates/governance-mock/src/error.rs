//! Error types for the mock governance implementation.

use proven_governance::{GovernanceError, GovernanceErrorKind};
use thiserror::Error;

/// Error type for the mock governance implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Returned when an operation is not implemented.
    #[error("Mock governance error: {0}")]
    MockError(String),

    /// Error related to private key operations.
    #[error("Private key error: {0}")]
    PrivateKey(String),

    /// Error when a required value is not initialized.
    #[error("Not initialized: {0}")]
    NotInitialized(String),

    /// Error when a node is not found in the topology.
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// Error when loading or parsing the topology file.
    #[error("Topology file error: {0}")]
    TopologyFile(String),
}

impl GovernanceError for Error {
    fn kind(&self) -> GovernanceErrorKind {
        match self {
            Self::PrivateKey(_) => GovernanceErrorKind::PrivateKey,
            Self::NotInitialized(_) => GovernanceErrorKind::NotInitialized,
            Self::NodeNotFound(_) => GovernanceErrorKind::NodeNotFound,
            Self::MockError(_) => GovernanceErrorKind::Other,
            Self::TopologyFile(_) => GovernanceErrorKind::Other,
        }
    }
}
