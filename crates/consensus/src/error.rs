//! Error types for the consensus system

use thiserror::Error;

/// Main consensus error type
#[derive(Error, Debug, Clone)]
pub enum ConsensusError {
    /// Network-related error
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Governance system error
    #[error("Governance error: {0}")]
    Governance(String),

    /// Attestation verification failed
    #[error("Attestation error: {0}")]
    Attestation(String),

    /// Invalid message format or content
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Consensus operation failed
    #[error("Consensus operation failed: {0}")]
    ConsensusFailed(String),

    /// Storage operation failed
    #[error("Storage error: {0}")]
    Storage(String),

    /// Raft-specific error
    #[error("Raft error: {0}")]
    Raft(String),

    /// Node not found in topology
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    /// Not enough nodes available to meet consensus requirements
    #[error("Insufficient nodes for consensus: available={available}, required={required}")]
    InsufficientNodes {
        /// Number of nodes currently available
        available: usize,
        /// Number of nodes required for consensus
        required: usize,
    },

    /// Cluster is in read-only mode
    #[error("Cluster is in read-only mode")]
    ReadOnly,

    /// Operation timed out
    #[error("Consensus timeout after {seconds} seconds")]
    Timeout {
        /// Timeout duration in seconds
        seconds: u64,
    },

    /// Write conflict detected
    #[error("Write conflict: expected sequence {expected}, got {actual}")]
    WriteConflict {
        /// Expected sequence number
        expected: u64,
        /// Actual sequence number found
        actual: u64,
    },

    /// Stream operation error
    #[error("Stream error: {0}")]
    Stream(String),

    /// Not currently the leader node
    #[error("Not leader: current leader is {leader:?}")]
    NotLeader {
        /// The current leader node ID, if known
        leader: Option<String>,
    },

    /// Catch-up synchronization error
    #[error("Catch-up error: {0}")]
    CatchUp(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Subject pattern validation error
    #[error("Invalid subject pattern: {0}")]
    InvalidSubjectPattern(String),

    /// Stream name validation error
    #[error("Invalid stream name: {0}")]
    InvalidStreamName(String),
}

/// Network-specific error type
#[derive(Error, Debug, Clone)]
pub enum NetworkError {
    /// Connection failed
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Peer not connected
    #[error("Peer not connected")]
    PeerNotConnected,

    /// Send operation failed
    #[error("Send failed: {0}")]
    SendFailed(String),

    /// Receive operation failed
    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    /// Address binding failed
    #[error("Bind failed: {0}")]
    BindFailed(String),

    /// Handshake failed
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    /// Transport not supported for this operation
    #[error("Transport not supported: {0}")]
    TransportNotSupported(String),

    /// Timeout occurred
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Raft error
    #[error("OpenRaft error: {0}")]
    Raft(#[from] openraft::error::NetworkError),
}

/// Result type for consensus operations
pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// Result type for network operations
pub type NetworkResult<T> = Result<T, NetworkError>;

impl From<std::io::Error> for NetworkError {
    fn from(err: std::io::Error) -> Self {
        NetworkError::ConnectionFailed(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for NetworkError {
    fn from(err: tokio::time::error::Elapsed) -> Self {
        NetworkError::Timeout(err.to_string())
    }
}

impl From<serde_json::Error> for ConsensusError {
    fn from(err: serde_json::Error) -> Self {
        ConsensusError::Serialization(err.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for ConsensusError {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        ConsensusError::Serialization(err.to_string())
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for ConsensusError {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        ConsensusError::Serialization(err.to_string())
    }
}
