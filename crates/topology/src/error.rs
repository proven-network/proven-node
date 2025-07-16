//! Simple error types for topology operations

use std::error::Error;
use std::fmt::{self, Debug};
use thiserror::Error as ThisError;

/// Topology-related errors
#[derive(Clone, Debug, ThisError)]
pub enum TopologyError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Governance error
    #[error("Governance error: {0}")]
    Governance(String),

    /// Topology adaptor error
    #[error("Topology adaptor error: {0}")]
    TopologyAdaptor(String),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Marker trait for `TopologyAdaptor` errors
pub trait TopologyAdaptorError: Debug + Error + Send + Sync {
    /// Returns the kind of this error
    fn kind(&self) -> TopologyAdaptorErrorKind;
}

/// The kind of topology adaptor error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TopologyAdaptorErrorKind {
    /// Error when a node is not found in the topology
    NodeNotFound,

    /// Error with contract/external service
    External,

    /// Other/unknown error
    Other,
}

impl fmt::Display for TopologyAdaptorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
