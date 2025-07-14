//! Generic transport abstraction for network communication
//!
//! This crate provides a transport-agnostic interface for network communication.
//! Specific transport implementations (TCP, WebSocket, etc.) are provided in separate crates.
//!
//! Transports handle:
//! - Message serialization and COSE signing
//! - Per-destination message queuing
//! - Connection management and retry logic
//! - Message deserialization and signature verification

pub mod error;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use error::TransportError;
use futures::Stream;
use proven_topology::NodeId;
use proven_verification::CoseHandler;
use std::pin::Pin;
use uuid::Uuid;

pub use error::TransportError as Error;

/// Transport-level message envelope containing verified message data
#[derive(Debug, Clone)]
pub struct TransportEnvelope {
    /// Correlation ID for request/response tracking
    pub correlation_id: Option<Uuid>,
    /// Message type extracted from COSE metadata
    pub message_type: String,
    /// The verified message payload
    pub payload: Bytes,
    /// The sender of the message
    pub sender: NodeId,
}

/// Transport trait for sending and receiving messages
///
/// Transports handle:
/// - COSE message signing and verification
/// - Connection management (on-demand via TopologyManager)
/// - Message queuing and delivery
/// - Retry logic and error recovery
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a message to a specific node
    ///
    /// The transport will:
    /// 1. Serialize and COSE-sign the message
    /// 2. Queue it for delivery (per-destination queues)
    /// 3. Establish connection if needed via TopologyManager
    /// 4. Handle retries on failure
    async fn send_envelope(
        &self,
        recipient: &NodeId,
        payload: &Bytes,
        message_type: &str,
        correlation_id: Option<Uuid>,
    ) -> Result<(), TransportError>;

    /// Get a stream of incoming message envelopes
    ///
    /// The transport will:
    /// 1. Receive and deserialize COSE messages
    /// 2. Verify signatures
    /// 3. Extract metadata and return typed envelopes
    fn incoming(&self) -> Pin<Box<dyn Stream<Item = TransportEnvelope> + Send>>;

    /// Get the COSE handler for this transport
    fn cose_handler(&self) -> &CoseHandler;

    /// Shutdown the transport
    async fn shutdown(&self) -> Result<(), TransportError>;
}

/// Configuration for transports
#[derive(Debug, Clone)]
pub struct Config {
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Keep-alive interval in milliseconds
    pub keep_alive_interval_ms: u64,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Message queue size per destination
    pub per_destination_queue_size: usize,
    /// Retry attempts for failed sends
    pub retry_attempts: usize,
    /// Delay between retries in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connection_timeout_ms: 5000,        // 5 seconds
            keep_alive_interval_ms: 30000,      // 30 seconds
            max_message_size: 10 * 1024 * 1024, // 10MB
            per_destination_queue_size: 1000,   // 1000 messages per destination
            retry_attempts: 3,
            retry_delay_ms: 500,
        }
    }
}

/// Trait for transports that can integrate with HTTP servers
pub trait HttpIntegratedTransport {
    /// Create an axum router for WebSocket integration
    fn create_router_integration(&self) -> Result<Router, TransportError>;

    /// Get the endpoint path
    fn endpoint(&self) -> &'static str;
}
