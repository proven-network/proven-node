//! Error types for the mock governance implementation.

use proven_governance::{GovernanceError, GovernanceErrorKind};
use thiserror::Error;

/// Error type for the mock governance implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Error when a node is not found in the topology.
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// Error when loading or parsing the topology file.
    #[error("Topology file error: {0}")]
    TopologyFile(String),

    /// Error when managing nodes in the topology.
    #[error("Node management error: {0}")]
    NodeManagement(String),
}

impl GovernanceError for Error {
    fn kind(&self) -> GovernanceErrorKind {
        match self {
            Self::NodeNotFound(_) => GovernanceErrorKind::NodeNotFound,
            Self::TopologyFile(_) | Self::NodeManagement(_) => GovernanceErrorKind::Other,
        }
    }
}
