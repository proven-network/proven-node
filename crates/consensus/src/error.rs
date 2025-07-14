//! Error types for the consensus system
//!
//! This module provides a comprehensive error hierarchy for all consensus operations,
//! with specific error types for different subsystems and failure modes.

use crate::ConsensusGroupId;
use proven_topology::NodeId;
use thiserror::Error;

/// Main consensus error type
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// Topology-related error
    #[error("Topology error: {0}")]
    Topology(#[from] proven_topology::TopologyError),

    /// Network-related error
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    /// Stream-related error
    #[error("Stream error: {0}")]
    Stream(#[from] StreamError),

    /// Group management error
    #[error("Group error: {0}")]
    Group(#[from] GroupError),

    /// Node management error
    #[error("Node error: {0}")]
    Node(#[from] NodeError),

    /// Storage operation failed
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(ConfigurationError),

    /// Governance system error
    #[error("Governance error: {0}")]
    Governance(String),

    /// Attestation verification failed
    #[error("Attestation error: {0}")]
    Attestation(String),

    /// Consensus operation failed
    #[error("Consensus operation failed: {0}")]
    ConsensusFailed(String),

    /// Raft-specific error
    #[error("Raft error: {0}")]
    Raft(String),

    /// Not currently the leader node
    #[error("Not leader: current leader is {leader:?}")]
    NotLeader {
        /// The current leader node ID, if known
        leader: Option<String>,
    },

    /// Operation timed out
    #[error("Consensus timeout after {seconds} seconds")]
    Timeout {
        /// Timeout duration in seconds
        seconds: u64,
    },

    /// Cluster is in read-only mode
    #[error("Cluster is in read-only mode")]
    ReadOnly,

    /// Invalid message format or content
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Migration failed
    #[error("Migration failed: {0}")]
    MigrationFailed(MigrationError),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Invalid state
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

/// Stream-specific errors
#[derive(Error, Debug, Clone)]
pub enum StreamError {
    /// Stream already exists
    #[error("Stream '{name}' already exists")]
    AlreadyExists {
        /// The name of the stream that already exists
        name: String,
    },

    /// Stream not found
    #[error("Stream '{name}' not found")]
    NotFound {
        /// The name of the stream that was not found
        name: String,
    },

    /// Invalid stream name
    #[error("Invalid stream name '{name}': {reason}")]
    InvalidName {
        /// The name of the stream that is invalid
        name: String,
        /// The reason the stream name is invalid
        reason: String,
    },

    /// Stream is not allocated to any group
    #[error("Stream '{name}' is not allocated to any consensus group")]
    NotAllocated {
        /// The name of the stream that is not allocated to any consensus group
        name: String,
    },

    /// Stream operation conflict
    #[error("Stream '{name}' operation conflict: {reason}")]
    OperationConflict {
        /// The name of the stream that is in conflict
        name: String,
        /// The reason the stream is in conflict
        reason: String,
    },

    /// Stream quota exceeded
    #[error("Stream quota exceeded: {reason}")]
    QuotaExceeded {
        /// The reason the stream quota was exceeded
        reason: String,
    },

    /// Invalid subject pattern
    #[error("Invalid subject pattern '{pattern}': {reason}")]
    InvalidSubjectPattern {
        /// The pattern that is invalid
        pattern: String,
        /// The reason the pattern is invalid
        reason: String,
    },

    /// Stream is being migrated
    #[error("Stream '{name}' is currently being migrated")]
    BeingMigrated {
        /// The name of the stream that is being migrated
        name: String,
    },
}

/// Group management errors
#[derive(Error, Debug, Clone)]
pub enum GroupError {
    /// Group already exists
    #[error("Consensus group {id:?} already exists")]
    AlreadyExists {
        /// The ID of the consensus group that already exists
        id: ConsensusGroupId,
    },

    /// Group not found
    #[error("Consensus group {id:?} not found")]
    NotFound {
        /// The ID of the consensus group that was not found
        id: ConsensusGroupId,
    },

    /// Group has active streams
    #[error("Consensus group {id:?} has {count} active streams")]
    HasActiveStreams {
        /// The ID of the consensus group that has active streams
        id: ConsensusGroupId,
        /// The number of active streams in the consensus group
        count: usize,
    },

    /// Group has no members
    #[error("Consensus group {id:?} has no members")]
    NoMembers {
        /// The ID of the consensus group that has no members
        id: ConsensusGroupId,
    },

    /// Group is full
    #[error("Consensus group {id:?} is at capacity ({max} members)")]
    AtCapacity {
        /// The ID of the consensus group that is at capacity
        id: ConsensusGroupId,
        /// The maximum number of members the consensus group can have
        max: usize,
    },

    /// No groups available
    #[error("No consensus groups available for allocation")]
    NoGroupsAvailable {
        /// The reason no consensus groups are available for allocation
        reason: String,
    },

    /// Invalid group operation
    #[error("Invalid group operation: {reason}")]
    InvalidOperation {
        /// The reason the group operation is invalid
        reason: String,
    },

    /// Insufficient members in group
    #[error("Insufficient members: need at least {required}, have {actual}")]
    InsufficientMembers {
        /// The number of members required
        required: usize,
        /// The number of members actually available
        actual: usize,
    },

    /// Too many members in group
    #[error("Too many members: maximum {max}, attempted {actual}")]
    TooManyMembers {
        /// The maximum number of members the consensus group can have
        max: usize,
        /// The number of members actually attempted
        actual: usize,
    },
}

/// Node management errors
#[derive(Error, Debug, Clone)]
pub enum NodeError {
    /// Node not found
    #[error("Node {id} not found")]
    NotFound {
        /// The ID of the node that was not found
        id: NodeId,
    },

    /// Node already in group
    #[error("Node {node_id} is already in group {group_id:?}")]
    AlreadyInGroup {
        /// The ID of the node that is already in the group
        node_id: NodeId,
        /// The ID of the consensus group that the node is already in
        group_id: ConsensusGroupId,
    },

    /// Node not in group
    #[error("Node {node_id} is not in group {group_id:?}")]
    NotInGroup {
        /// The ID of the node that is not in the group
        node_id: NodeId,
        /// The ID of the consensus group that the node is not in
        group_id: ConsensusGroupId,
    },

    /// Insufficient nodes for operation
    #[error("Insufficient nodes: need {required}, have {available}")]
    InsufficientNodes {
        /// The number of nodes required
        required: usize,
        /// The number of nodes actually available
        available: usize,
    },

    /// Node is offline
    #[error("Node {id} is offline")]
    Offline {
        /// The ID of the node that is offline
        id: NodeId,
    },

    /// Node capacity exceeded
    #[error("Node {id} is at capacity")]
    AtCapacity {
        /// The ID of the node that is at capacity
        id: NodeId,
    },

    /// Node has too many groups
    #[error("Node {node_id} has too many groups: {current}/{max}")]
    TooManyGroups {
        /// The ID of the node that has too many groups
        node_id: NodeId,
        /// The number of groups the node is currently in
        current: usize,
        /// The maximum number of groups the node can be in
        max: usize,
    },
}

/// Storage-specific errors
#[derive(Error, Debug, Clone)]
pub enum StorageError {
    /// Storage backend unavailable
    #[error("Storage backend unavailable: {backend}")]
    BackendUnavailable {
        /// The name of the storage backend that is unavailable
        backend: String,
    },

    /// Storage corruption detected
    #[error("Storage corruption detected: {details}")]
    Corruption {
        /// The details of the storage corruption
        details: String,
    },

    /// Storage operation failed
    #[error("Storage operation failed: {operation}")]
    OperationFailed {
        /// The operation that failed
        operation: String,
    },

    /// Out of storage space
    #[error("Out of storage space")]
    OutOfSpace,

    /// Invalid storage key
    #[error("Invalid storage key: {key}")]
    InvalidKey {
        /// The key that is invalid
        key: String,
    },

    /// Checksum mismatch
    #[error("Checksum mismatch for key {key}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// The key that has a checksum mismatch
        key: String,
        /// The expected checksum
        expected: String,
        /// The actual checksum
        actual: String,
    },
}

/// Configuration errors
#[derive(Error, Debug, Clone)]
pub enum ConfigurationError {
    /// Invalid configuration value
    #[error("Invalid configuration value for '{key}': {reason}")]
    InvalidValue {
        /// The key that has an invalid value
        key: String,
        /// The reason the value is invalid
        reason: String,
    },

    /// Missing required configuration
    #[error("Missing required configuration: {key}")]
    MissingRequired {
        /// The key that is missing
        key: String,
    },

    /// Configuration conflict
    #[error("Configuration conflict: {details}")]
    Conflict {
        /// The details of the configuration conflict
        details: String,
    },

    /// Invalid transport configuration
    #[error("Invalid transport configuration: {reason}")]
    InvalidTransport {
        /// The reason the transport configuration is invalid
        reason: String,
    },

    /// Invalid storage configuration
    #[error("Invalid storage configuration: {reason}")]
    InvalidStorage {
        /// The reason the storage configuration is invalid
        reason: String,
    },
}

/// Migration-specific errors
#[derive(Error, Debug, Clone)]
pub enum MigrationError {
    /// Migration already in progress
    #[error("Migration already in progress for stream '{stream}'")]
    AlreadyInProgress {
        /// The name of the stream that is already in progress
        stream: String,
    },

    /// Invalid migration state
    #[error("Invalid migration state: {details}")]
    InvalidState {
        /// The details of the invalid migration state
        details: String,
    },

    /// Migration timeout
    #[error("Migration timeout for stream '{stream}' after {seconds}s")]
    Timeout {
        /// The name of the stream that timed out
        stream: String,
        /// The timeout duration in seconds
        seconds: u64,
    },

    /// Data loss detected during migration
    #[error("Data loss detected during migration: {details}")]
    DataLoss {
        /// The details of the data loss
        details: String,
    },

    /// Target group unavailable
    #[error("Target group {group_id:?} unavailable for migration")]
    TargetUnavailable {
        /// The ID of the consensus group that is unavailable
        group_id: ConsensusGroupId,
    },
}

/// Network-specific error type
#[derive(Error, Debug, Clone)]
pub enum NetworkError {
    /// Connection failed
    #[error("Connection to {peer} failed: {reason}")]
    ConnectionFailed {
        /// The ID of the peer that failed to connect
        peer: NodeId,
        /// The reason the connection failed
        reason: String,
    },

    /// Peer not connected
    #[error("Peer {peer} not connected")]
    PeerNotConnected {
        /// The ID of the peer that is not connected
        peer: NodeId,
    },

    /// Send operation failed
    #[error("Failed to send to {peer}: {reason}")]
    SendFailed {
        /// The ID of the peer that failed to send
        peer: NodeId,
        /// The reason the send failed
        reason: String,
    },

    /// Receive operation failed
    #[error("Failed to receive: {reason}")]
    ReceiveFailed {
        /// The reason the receive failed
        reason: String,
    },

    /// Address binding failed
    #[error("Failed to bind to {address}: {reason}")]
    BindFailed {
        /// The address that failed to bind
        address: String,
        /// The reason the bind failed
        reason: String,
    },

    /// Handshake failed
    #[error("Handshake with {peer} failed: {reason}")]
    HandshakeFailed {
        /// The ID of the peer that failed to handshake
        peer: String,
        /// The reason the handshake failed
        reason: String,
    },

    /// Transport not supported for this operation
    #[error("Transport {transport} not supported for {operation}")]
    TransportNotSupported {
        /// The transport that is not supported
        transport: String,
        /// The operation that is not supported
        operation: String,
    },

    /// Timeout occurred
    #[error("Network timeout: {operation}")]
    Timeout {
        /// The operation that timed out
        operation: String,
    },

    /// Raft network error
    #[error("OpenRaft network error: {0}")]
    Raft(#[from] openraft::error::NetworkError),
}

/// Result type for consensus operations
pub type ConsensusResult<T> = Result<T, Error>;

/// Result type for network operations
pub type NetworkResult<T> = Result<T, NetworkError>;

// Conversion implementations

// Implement conversion to network error for handler compatibility
impl From<Error> for proven_network::NetworkError {
    fn from(err: Error) -> Self {
        // Since Error implements std::error::Error, we can box it
        proven_network::NetworkError::Handler(Box::new(err))
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(err: std::io::Error) -> Self {
        NetworkError::ConnectionFailed {
            peer: NodeId::from_seed(0),
            reason: err.to_string(),
        }
    }
}

impl From<tokio::time::error::Elapsed> for NetworkError {
    fn from(_err: tokio::time::error::Elapsed) -> Self {
        NetworkError::Timeout {
            operation: "unknown".to_string(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<ConfigurationError> for Error {
    fn from(err: ConfigurationError) -> Self {
        Error::Configuration(err)
    }
}

impl From<MigrationError> for Error {
    fn from(err: MigrationError) -> Self {
        Error::MigrationFailed(err)
    }
}

// Backwards compatibility helpers

impl Error {
    /// Create an invalid stream name error (for backwards compatibility)
    pub fn invalid_stream_name(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Error::Stream(StreamError::InvalidName {
            name: String::new(),
            reason: msg,
        })
    }

    /// Create a not found error (for backwards compatibility)
    pub fn not_found(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        // Try to determine what type of not found
        if msg.contains("stream") || msg.contains("Stream") {
            Error::Stream(StreamError::NotFound { name: msg })
        } else if msg.contains("group") || msg.contains("Group") {
            Error::Group(GroupError::NotFound {
                id: ConsensusGroupId::new(0), // Placeholder
            })
        } else if msg.contains("node") || msg.contains("Node") {
            Error::Node(NodeError::NotFound {
                id: NodeId::from_seed(0), // Placeholder
            })
        } else {
            Error::InvalidOperation(msg)
        }
    }

    /// Create an already exists error (for backwards compatibility)
    pub fn already_exists(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        if msg.contains("stream") || msg.contains("Stream") {
            Error::Stream(StreamError::AlreadyExists { name: msg })
        } else if msg.contains("group") || msg.contains("Group") {
            Error::Group(GroupError::AlreadyExists {
                id: ConsensusGroupId::new(0), // Placeholder
            })
        } else {
            Error::InvalidOperation(msg)
        }
    }

    /// Create a storage error (for backwards compatibility)
    pub fn storage(msg: impl Into<String>) -> Self {
        Error::Storage(StorageError::OperationFailed {
            operation: msg.into(),
        })
    }
}

// Re-export common constructors for backwards compatibility
#[allow(non_snake_case)]
impl Error {
    /// Create a not found error (for backwards compatibility)
    pub fn NotFound(msg: String) -> Self {
        Self::not_found(msg)
    }

    /// Create an already exists error (for backwards compatibility)
    pub fn AlreadyExists(msg: String) -> Self {
        Self::already_exists(msg)
    }

    /// Create a storage error (for backwards compatibility)
    pub fn Storage(msg: String) -> Self {
        Self::storage(msg)
    }

    /// Create an invalid stream name error (for backwards compatibility)
    pub fn InvalidStreamName(msg: String) -> Self {
        Self::invalid_stream_name(msg)
    }

    /// Create an invalid subject pattern error (for backwards compatibility)
    pub fn InvalidSubjectPattern(msg: String) -> Self {
        Error::Stream(StreamError::InvalidSubjectPattern {
            pattern: String::new(),
            reason: msg,
        })
    }

    /// Create a read-only error (for backwards compatibility)
    pub fn ReadOnly() -> Self {
        Error::ReadOnly
    }
}

// Backwards compatibility for NetworkError
impl NetworkError {
    /// Create a send failed error (for backwards compatibility)
    pub fn send_failed(msg: impl Into<String>) -> Self {
        NetworkError::SendFailed {
            peer: NodeId::from_seed(0), // Placeholder
            reason: msg.into(),
        }
    }

    /// Create a peer not connected error (for backwards compatibility)
    pub fn peer_not_connected() -> Self {
        NetworkError::PeerNotConnected {
            peer: NodeId::from_seed(0), // Placeholder
        }
    }

    /// Create a transport not supported error (for backwards compatibility)
    pub fn transport_not_supported(msg: impl Into<String>) -> Self {
        NetworkError::TransportNotSupported {
            transport: "unknown".to_string(),
            operation: msg.into(),
        }
    }
}

// Re-export common constructors for backwards compatibility
#[allow(non_snake_case)]
impl NetworkError {
    /// Create a send failed error (for backwards compatibility)
    pub fn SendFailed(msg: String) -> Self {
        Self::send_failed(msg)
    }

    /// Create a peer not connected error (for backwards compatibility)
    pub fn PeerNotConnected() -> Self {
        Self::peer_not_connected()
    }

    /// Create a transport not supported error (for backwards compatibility)
    pub fn TransportNotSupported(msg: String) -> Self {
        Self::transport_not_supported(msg)
    }
}
