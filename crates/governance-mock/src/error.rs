//! Error types for the mock governance implementation.

use proven_governance::GovernanceError;
use thiserror::Error;

/// Error type for the mock governance implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Returned when an operation is not implemented.
    #[error("Mock governance error: {0}")]
    MockError(String),
}

impl GovernanceError for Error {}
