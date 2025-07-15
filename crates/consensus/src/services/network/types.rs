//! Types for the network service

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::foundation::types::ConsensusGroupId;
use proven_topology::NodeId;

/// Result type for network operations
pub type NetworkResult<T> = Result<T, NetworkError>;

/// Network service error
#[derive(Debug, Error)]
pub enum NetworkError {
    /// Transport error
    #[error("Transport error: {0}")]
    Transport(String),

    /// Connection error
    #[error("Connection error to {node}: {error}")]
    Connection { node: NodeId, error: String },

    /// Timeout error
    #[error("Network timeout")]
    Timeout,

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Not connected
    #[error("Not connected to node {0}")]
    NotConnected(NodeId),

    /// Internal error
    #[error("Internal network error: {0}")]
    Internal(String),
}

/// Network service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,

    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,

    /// Maximum retry attempts
    pub max_retries: u32,

    /// Retry backoff in milliseconds
    pub retry_backoff_ms: u64,

    /// Maximum concurrent connections
    pub max_connections: usize,

    /// Connection pool size per node
    pub pool_size_per_node: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connection_timeout_ms: 5000,
            request_timeout_ms: 10000,
            max_retries: 3,
            retry_backoff_ms: 1000,
            max_connections: 100,
            pool_size_per_node: 5,
        }
    }
}

/// Network events
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// Node connected
    NodeConnected(NodeId),

    /// Node disconnected
    NodeDisconnected(NodeId),

    /// Connection state changed
    ConnectionStateChanged {
        node: NodeId,
        state: ConnectionState,
    },
}

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// Disconnected
    Disconnected,

    /// Connecting
    Connecting,

    /// Connected
    Connected,

    /// Failed
    Failed,
}

/// Network statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Messages sent
    pub messages_sent: u64,
    /// Messages received
    pub messages_received: u64,
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Active connections
    pub active_connections: usize,
    /// Failed connections
    pub failed_connections: u64,
}
