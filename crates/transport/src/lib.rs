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

pub mod connection;
pub mod error;
pub mod verification;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use error::TransportError;
use futures::Stream;
use proven_topology::NodeId;
use std::pin::Pin;
use uuid::Uuid;

pub use error::TransportError as Error;

// Re-export verification types for compatibility and convenience
pub use verification::{
    AttestationVerifier, Bytes as VerificationBytes, ConnectionVerifier, CoseHandler, CoseMetadata,
    CoseSign1, VerificationError, VerificationMessage, VerificationResult,
};

// Re-export connection management types
pub use connection::{ConnectionConfig, ConnectionManager};

/// Transport-level message envelope containing verified message data
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    /// Shutdown the transport
    async fn shutdown(&self) -> Result<(), TransportError>;
}

/// Configuration for transports
#[derive(Debug, Clone)]
pub struct Config {
    /// Connection-specific configuration
    pub connection: ConnectionConfig,
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
            connection: ConnectionConfig::default(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[proven_logger::logged_test]
    fn test_config_default() {
        let config = Config::default();

        // Test that connection config is properly included
        assert_eq!(config.connection.connection_timeout, Duration::from_secs(5));
        assert_eq!(
            config.connection.verification_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(config.connection.max_verification_attempts, 3);
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(300));
        assert_eq!(
            config.connection.keep_alive_interval,
            Duration::from_secs(60)
        );

        // Test other config fields
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
        assert_eq!(config.per_destination_queue_size, 1000);
        assert_eq!(config.retry_attempts, 3);
        assert_eq!(config.retry_delay_ms, 500);
    }

    #[proven_logger::logged_test]
    fn test_config_with_custom_connection_config() {
        let custom_connection_config = ConnectionConfig {
            connection_timeout: Duration::from_secs(10),
            verification_timeout: Duration::from_secs(60),
            max_verification_attempts: 5,
            idle_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(120),
        };

        let config = Config {
            connection: custom_connection_config.clone(),
            max_message_size: 5 * 1024 * 1024,
            per_destination_queue_size: 500,
            retry_attempts: 5,
            retry_delay_ms: 1000,
        };

        assert_eq!(
            config.connection.connection_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(
            config.connection.verification_timeout,
            Duration::from_secs(60)
        );
        assert_eq!(config.connection.max_verification_attempts, 5);
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(600));
        assert_eq!(
            config.connection.keep_alive_interval,
            Duration::from_secs(120)
        );
        assert_eq!(config.max_message_size, 5 * 1024 * 1024);
        assert_eq!(config.per_destination_queue_size, 500);
        assert_eq!(config.retry_attempts, 5);
        assert_eq!(config.retry_delay_ms, 1000);
    }

    #[proven_logger::logged_test]
    fn test_connection_config_validation() {
        // Test valid configuration
        let valid_config = ConnectionConfig {
            connection_timeout: Duration::from_secs(5),
            verification_timeout: Duration::from_secs(30),
            max_verification_attempts: 3,
            idle_timeout: Duration::from_secs(300),
            keep_alive_interval: Duration::from_secs(60),
        };

        // Should be able to create and use the config
        assert_eq!(valid_config.connection_timeout, Duration::from_secs(5));
        assert_eq!(valid_config.verification_timeout, Duration::from_secs(30));
        assert_eq!(valid_config.max_verification_attempts, 3);
        assert_eq!(valid_config.idle_timeout, Duration::from_secs(300));
        assert_eq!(valid_config.keep_alive_interval, Duration::from_secs(60));
    }

    #[proven_logger::logged_test]
    fn test_connection_config_extreme_values() {
        // Test with very short timeouts
        let short_timeout_config = ConnectionConfig {
            connection_timeout: Duration::from_millis(50),
            verification_timeout: Duration::from_millis(100),
            max_verification_attempts: 1,
            idle_timeout: Duration::from_millis(500),
            keep_alive_interval: Duration::from_millis(50),
        };

        assert_eq!(
            short_timeout_config.connection_timeout,
            Duration::from_millis(50)
        );
        assert_eq!(
            short_timeout_config.verification_timeout,
            Duration::from_millis(100)
        );
        assert_eq!(short_timeout_config.max_verification_attempts, 1);
        assert_eq!(
            short_timeout_config.idle_timeout,
            Duration::from_millis(500)
        );
        assert_eq!(
            short_timeout_config.keep_alive_interval,
            Duration::from_millis(50)
        );

        // Test with very long timeouts
        let long_timeout_config = ConnectionConfig {
            connection_timeout: Duration::from_secs(300), // 5 minutes
            verification_timeout: Duration::from_secs(3600), // 1 hour
            max_verification_attempts: 100,
            idle_timeout: Duration::from_secs(86400), // 24 hours
            keep_alive_interval: Duration::from_secs(1800), // 30 minutes
        };

        assert_eq!(
            long_timeout_config.connection_timeout,
            Duration::from_secs(300)
        );
        assert_eq!(
            long_timeout_config.verification_timeout,
            Duration::from_secs(3600)
        );
        assert_eq!(long_timeout_config.max_verification_attempts, 100);
        assert_eq!(long_timeout_config.idle_timeout, Duration::from_secs(86400));
        assert_eq!(
            long_timeout_config.keep_alive_interval,
            Duration::from_secs(1800)
        );
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_creation() {
        let node_id = proven_topology::NodeId::from_seed(1);
        let correlation_id = Uuid::new_v4();

        let envelope = TransportEnvelope {
            correlation_id: Some(correlation_id),
            message_type: "test_message".to_string(),
            payload: Bytes::from("test payload data"),
            sender: node_id.clone(),
        };

        assert_eq!(envelope.correlation_id, Some(correlation_id));
        assert_eq!(envelope.message_type, "test_message");
        assert_eq!(envelope.payload, Bytes::from("test payload data"));
        assert_eq!(envelope.sender, node_id);
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_without_correlation_id() {
        let node_id = proven_topology::NodeId::from_seed(2);

        let envelope = TransportEnvelope {
            correlation_id: None,
            message_type: "notification".to_string(),
            payload: Bytes::from("notification data"),
            sender: node_id.clone(),
        };

        assert_eq!(envelope.correlation_id, None);
        assert_eq!(envelope.message_type, "notification");
        assert_eq!(envelope.payload, Bytes::from("notification data"));
        assert_eq!(envelope.sender, node_id);
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_with_empty_payload() {
        let node_id = proven_topology::NodeId::from_seed(3);

        let envelope = TransportEnvelope {
            correlation_id: Some(Uuid::new_v4()),
            message_type: "ping".to_string(),
            payload: Bytes::new(),
            sender: node_id.clone(),
        };

        assert!(envelope.payload.is_empty());
        assert_eq!(envelope.message_type, "ping");
        assert_eq!(envelope.sender, node_id);
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_with_large_payload() {
        let node_id = proven_topology::NodeId::from_seed(4);

        // Create a large payload (1MB)
        let large_payload = vec![0u8; 1024 * 1024];
        let payload_bytes = Bytes::from(large_payload);

        let envelope = TransportEnvelope {
            correlation_id: Some(Uuid::new_v4()),
            message_type: "large_data".to_string(),
            payload: payload_bytes.clone(),
            sender: node_id.clone(),
        };

        assert_eq!(envelope.payload.len(), 1024 * 1024);
        assert_eq!(envelope.payload, payload_bytes);
        assert_eq!(envelope.message_type, "large_data");
        assert_eq!(envelope.sender, node_id);
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_clone() {
        let node_id = proven_topology::NodeId::from_seed(5);
        let correlation_id = Uuid::new_v4();

        let envelope = TransportEnvelope {
            correlation_id: Some(correlation_id),
            message_type: "cloneable".to_string(),
            payload: Bytes::from("clone test data"),
            sender: node_id.clone(),
        };

        let cloned_envelope = envelope.clone();

        assert_eq!(envelope.correlation_id, cloned_envelope.correlation_id);
        assert_eq!(envelope.message_type, cloned_envelope.message_type);
        assert_eq!(envelope.payload, cloned_envelope.payload);
        assert_eq!(envelope.sender, cloned_envelope.sender);
    }

    #[proven_logger::logged_test]
    fn test_transport_envelope_debug() {
        let node_id = proven_topology::NodeId::from_seed(6);

        let envelope = TransportEnvelope {
            correlation_id: Some(Uuid::new_v4()),
            message_type: "debug_test".to_string(),
            payload: Bytes::from("debug data"),
            sender: node_id,
        };

        let debug_str = format!("{envelope:?}");

        assert!(debug_str.contains("TransportEnvelope"));
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("debug data"));
    }

    #[proven_logger::logged_test]
    fn test_config_clone() {
        let config = Config::default();
        let cloned_config = config.clone();

        assert_eq!(
            config.connection.verification_timeout,
            cloned_config.connection.verification_timeout
        );
        assert_eq!(
            config.connection.max_verification_attempts,
            cloned_config.connection.max_verification_attempts
        );
        assert_eq!(
            config.connection.idle_timeout,
            cloned_config.connection.idle_timeout
        );
        assert_eq!(
            config.connection.keep_alive_interval,
            cloned_config.connection.keep_alive_interval
        );
        assert_eq!(config.max_message_size, cloned_config.max_message_size);
        assert_eq!(
            config.per_destination_queue_size,
            cloned_config.per_destination_queue_size
        );
        assert_eq!(config.retry_attempts, cloned_config.retry_attempts);
        assert_eq!(config.retry_delay_ms, cloned_config.retry_delay_ms);
    }

    #[proven_logger::logged_test]
    fn test_config_debug() {
        let config = Config::default();
        let debug_str = format!("{config:?}");

        assert!(debug_str.contains("Config"));
        assert!(debug_str.contains("connection"));
        assert!(debug_str.contains("max_message_size"));
        assert!(debug_str.contains("per_destination_queue_size"));
        assert!(debug_str.contains("retry_attempts"));
        assert!(debug_str.contains("retry_delay_ms"));
    }

    #[proven_logger::logged_test]
    fn test_connection_config_clone() {
        let config = ConnectionConfig::default();
        let cloned_config = config.clone();

        assert_eq!(
            config.verification_timeout,
            cloned_config.verification_timeout
        );
        assert_eq!(
            config.max_verification_attempts,
            cloned_config.max_verification_attempts
        );
        assert_eq!(config.idle_timeout, cloned_config.idle_timeout);
        assert_eq!(
            config.keep_alive_interval,
            cloned_config.keep_alive_interval
        );
    }

    #[proven_logger::logged_test]
    fn test_connection_config_debug() {
        let config = ConnectionConfig::default();
        let debug_str = format!("{config:?}");

        assert!(debug_str.contains("ConnectionConfig"));
        assert!(debug_str.contains("verification_timeout"));
        assert!(debug_str.contains("max_verification_attempts"));
        assert!(debug_str.contains("idle_timeout"));
        assert!(debug_str.contains("keep_alive_interval"));
    }

    #[proven_logger::logged_test]
    fn test_envelope_with_different_message_types() {
        let node_id = proven_topology::NodeId::from_seed(7);

        let message_types = vec![
            "request",
            "response",
            "notification",
            "heartbeat",
            "error",
            "data_sync",
            "auth_challenge",
            "auth_response",
        ];

        for message_type in message_types {
            let envelope = TransportEnvelope {
                correlation_id: Some(Uuid::new_v4()),
                message_type: message_type.to_string(),
                payload: Bytes::from(format!("{message_type} payload")),
                sender: node_id.clone(),
            };

            assert_eq!(envelope.message_type, message_type);
            assert_eq!(
                envelope.payload,
                Bytes::from(format!("{message_type} payload"))
            );
        }
    }
}
