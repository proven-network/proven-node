use eyre::Report;
use proven_topology::{TopologyAdaptorError, TopologyAdaptorErrorKind};
use thiserror::Error;

/// Errors that can occur when interacting with the Helios topology adaptor client.
#[derive(Debug, Error)]
pub enum Error {
    /// Error decoding contract data
    #[error("Contract data decode error: {0}")]
    ContractDataDecode(String),

    /// Error from the Helios client
    #[error("Helios client error: {0}")]
    Helios(#[from] Report),

    /// Error parsing contract address
    #[error("Invalid address for {0}")]
    InvalidAddress(String),

    /// Error when a node is not found in the topology
    #[error("Node not found: {0}")]
    NodeNotFound(String),
}

impl TopologyAdaptorError for Error {
    fn kind(&self) -> TopologyAdaptorErrorKind {
        match self {
            Self::ContractDataDecode(_) | Self::Helios(_) | Self::InvalidAddress(_) => {
                TopologyAdaptorErrorKind::External
            }
            Self::NodeNotFound(_) => TopologyAdaptorErrorKind::NodeNotFound,
        }
    }
}
