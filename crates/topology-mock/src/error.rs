//! Error types for the mock topology adaptor implementation.

use proven_topology::{TopologyAdaptorError, TopologyAdaptorErrorKind};
use thiserror::Error;

/// Error type for the mock topology adaptor implementation.
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

impl TopologyAdaptorError for Error {
    fn kind(&self) -> TopologyAdaptorErrorKind {
        match self {
            Self::NodeNotFound(_) => TopologyAdaptorErrorKind::NodeNotFound,
            Self::TopologyFile(_) | Self::NodeManagement(_) => TopologyAdaptorErrorKind::Other,
        }
    }
}
