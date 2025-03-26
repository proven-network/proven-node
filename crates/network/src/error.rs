//! Error types for the network implementation.

use thiserror::Error;

/// Result type for the network implementation.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for the network implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Attestation error.
    #[error("attestation error: {0}")]
    Attestation(String),

    /// Bad origin error.
    #[error("the origin is invalid")]
    BadOrigin,

    /// Error from the governance implementation.
    #[error("governance error: {0}")]
    Governance(String),

    /// Error when a node is not found in the topology.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// Error related to private key operations.
    #[error("private key error: {0}")]
    PrivateKey(String),

    /// Error from reqwest.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    /// Request failed with non-200 status code.
    #[error("request failed with status code: {0}")]
    RequestFailed(u16),

    /// URL parsing error.
    #[error(transparent)]
    Url(#[from] url::ParseError),
}
