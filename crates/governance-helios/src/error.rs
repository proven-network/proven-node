use eyre::Report;
use proven_governance::{GovernanceError, GovernanceErrorKind};
use thiserror::Error;

/// Errors that can occur when interacting with the Helios governance client.
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

impl GovernanceError for Error {
    fn kind(&self) -> GovernanceErrorKind {
        match self {
            Self::ContractDataDecode(_) | Self::Helios(_) | Self::InvalidAddress(_) => {
                GovernanceErrorKind::External
            }
            Self::NodeNotFound(_) => GovernanceErrorKind::NodeNotFound,
        }
    }
}
