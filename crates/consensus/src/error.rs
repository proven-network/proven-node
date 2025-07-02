//! Error types for the consensus system.

/// Result type for consensus operations.
pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// Errors that can occur in the consensus system.
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// Error from the governance system.
    #[error("Governance error: {0}")]
    Governance(String),

    /// Network error during peer communication.
    #[error("Network error: {0}")]
    Network(Box<dyn std::error::Error + Send + Sync>),

    /// Consensus protocol error.
    #[error("Consensus error: {0}")]
    Consensus(String),

    /// Node not found in topology.
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// Not enough nodes available to meet consensus requirements.
    #[error("Insufficient nodes for consensus: available={available}, required={required}")]
    InsufficientNodes {
        /// Number of nodes currently available
        available: usize,
        /// Number of nodes required for consensus
        required: usize,
    },

    /// Cluster is in read-only mode.
    #[error("Cluster is in read-only mode")]
    ReadOnly,

    /// Operation timed out.
    #[error("Consensus timeout after {seconds} seconds")]
    Timeout {
        /// Timeout duration in seconds
        seconds: u64,
    },

    /// Write conflict detected.
    #[error("Write conflict: expected sequence {expected}, got {actual}")]
    WriteConflict {
        /// Expected sequence number
        expected: u64,
        /// Actual sequence number found
        actual: u64,
    },

    /// Stream operation error.
    #[error("Stream error: {0}")]
    Stream(String),

    /// Invalid request or message format.
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),

    /// Not currently the leader node.
    #[error("Not leader: current leader is {leader:?}")]
    NotLeader {
        /// The current leader node ID, if known
        leader: Option<String>,
    },

    /// Catch-up synchronization error.
    #[error("Catch-up error: {0}")]
    CatchUp(String),

    /// Attestation verification failure.
    #[error("Attestation error: {0}")]
    AttestationFailure(String),

    /// Governance system error.
    #[error("Governance failure: {0}")]
    GovernanceFailure(String),

    /// Raft consensus error.
    #[error("Raft error: {0}")]
    RaftError(String),

    /// Configuration error.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
}
