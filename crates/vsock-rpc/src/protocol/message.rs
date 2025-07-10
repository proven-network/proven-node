//! Core message traits and types.

use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// Unique identifier for a message type.
pub type MessageId = &'static str;

/// Base trait for RPC messages.
///
/// This trait must be implemented for all message types that will be
/// sent through the RPC system. It provides type safety by associating
/// each request type with its corresponding response type.
pub trait RpcMessage: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The response type for this message.
    type Response: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Returns a unique identifier for this message type.
    ///
    /// This is used for routing and logging purposes.
    fn message_id(&self) -> MessageId;

    /// Returns the expected response message ID.
    ///
    /// By default, this returns `{message_id}.response`.
    fn response_id(&self) -> MessageId {
        match self.message_id() {
            "ping" => "pong",
            id => Box::leak(format!("{id}.response").into_boxed_str()),
        }
    }
}

/// A simple ping message for health checks.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Ping {
    /// Timestamp when the ping was sent.
    pub timestamp: u64,
    /// Optional payload for echo testing.
    pub payload: Option<Vec<u8>>,
}

/// Response to a ping message.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Pong {
    /// Original timestamp from the ping.
    pub timestamp: u64,
    /// Echo of the original payload.
    pub payload: Option<Vec<u8>>,
    /// Server timestamp when pong was sent.
    pub server_timestamp: u64,
}

impl RpcMessage for Ping {
    type Response = Pong;

    fn message_id(&self) -> MessageId {
        "ping"
    }
}

/// Internal message envelope for wire format.
#[derive(Debug, Serialize, serde::Deserialize)]
pub(crate) struct MessageEnvelope {
    /// Unique request ID for correlation.
    pub id: uuid::Uuid,
    /// Message type identifier.
    pub message_id: String,
    /// Serialized message payload.
    pub payload: Vec<u8>,
    /// Optional checksum for integrity.
    pub checksum: Option<u32>,
}

/// Response envelope for wire format.
#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ResponseEnvelope {
    /// Request ID this response is for.
    pub request_id: uuid::Uuid,
    /// Response type identifier.
    pub message_id: String,
    /// Serialized response payload.
    pub payload: Vec<u8>,
    /// Optional error if request failed.
    pub error: Option<ErrorInfo>,
}

/// Error information for failed requests.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ErrorInfo {
    /// Error code for categorization.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Optional additional details.
    pub details: Option<serde_json::Value>,
}
