//! Transport layer for consensus networking
//!
//! This module provides a clean separation between networking and business logic.
//! Transports are responsible only for sending/receiving bytes, while consensus
//! handles all message interpretation, COSE signing, attestation, etc.

pub mod tcp;
pub mod websocket;

use crate::NodeId;
use crate::error::NetworkResult;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;

/// Callback function for handling incoming messages
pub type MessageHandler = Arc<dyn Fn(NodeId, Bytes) + Send + Sync>;

pub use tcp::TcpTransport;
pub use websocket::WebSocketTransport;

// Re-export TransportConfig from config module for convenience
pub use crate::config::TransportConfig;

/// Information about a peer connection
#[derive(Debug, Clone)]
pub struct PeerConnection<T> {
    /// Peer's node ID
    pub node_id: NodeId,
    /// Whether the connection is active
    pub connected: bool,
    /// Last activity timestamp
    pub last_activity: SystemTime,
    /// The actual connection (generic over connection type)
    pub connection: T,
}

/// Core transport trait for pure networking operations
#[async_trait]
pub trait NetworkTransport: Send + Sync + Debug + 'static {
    /// Send raw bytes to a target node
    async fn send_bytes(&self, target_node_id: &NodeId, data: Bytes) -> NetworkResult<()>;

    /// Start listening for incoming connections with the provided message handler
    async fn start_listener(&self, message_handler: MessageHandler) -> NetworkResult<()>;

    /// Shutdown the transport
    async fn shutdown(&self) -> NetworkResult<()>;

    /// Get list of connected peers
    async fn get_connected_peers(&self) -> NetworkResult<Vec<(NodeId, bool, SystemTime)>> {
        // Return basic peer info as tuples (node_id, connected, last_activity)
        // Each transport will implement this differently
        Ok(vec![])
    }

    /// Type erasure helper for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Optional trait for transports that can integrate with HTTP servers
pub trait HttpIntegratedTransport: NetworkTransport {
    /// Get the WebSocket endpoint path for this transport
    fn websocket_endpoint(&self) -> &'static str {
        "/consensus/{node_id}"
    }

    /// Create an Axum router for HTTP integration
    /// Note: The actual business logic will be provided by consensus layer
    fn create_router_integration(&self) -> NetworkResult<Router>;
}
