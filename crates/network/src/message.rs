//! Network message types and traits

use std::any::Any;
use std::fmt::Debug;

use bytes::Bytes;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::namespace::MessageType;

/// Base trait for all network messages
pub trait NetworkMessage: Send + Sync + Any + Debug + 'static {
    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn Any;

    /// Get the message type identifier for routing
    fn message_type(&self) -> &'static str;

    /// Serialize the message to bytes
    fn serialize(&self) -> Result<Bytes, crate::error::NetworkError>;
}

/// Trait for messages that expect a response
pub trait HandledMessage: NetworkMessage {
    /// The response type for this message
    type Response: NetworkMessage;
}

// Blanket implementation for types that implement MessageType + Serialize
impl<T> NetworkMessage for T
where
    T: MessageType + Serialize + DeserializeOwned + Send + Sync + Any + Debug + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn message_type(&self) -> &'static str {
        MessageType::message_type(self)
    }

    fn serialize(&self) -> Result<Bytes, crate::error::NetworkError> {
        let mut bytes = Vec::new();
        ciborium::into_writer(self, &mut bytes).map_err(|e| {
            crate::error::NetworkError::Serialization(format!("Failed to serialize message: {e}"))
        })?;
        Ok(Bytes::from(bytes))
    }
}

/// Network envelope containing message metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkEnvelope {
    /// Optional correlation ID for request/response tracking
    pub correlation_id: Option<Uuid>,
    /// Message type identifier
    pub message_type: String,
    /// The message payload (serialized)
    pub payload: Bytes,
    /// The sender of the message
    pub sender: NodeId,
}

impl NetworkEnvelope {
    /// Create a new network envelope
    pub fn new(sender: NodeId, payload: Bytes, message_type: String) -> Self {
        Self {
            correlation_id: None,
            message_type,
            payload,
            sender,
        }
    }

    /// Create a new request envelope with correlation ID
    pub fn request(sender: NodeId, payload: Bytes, message_type: String) -> Self {
        Self {
            correlation_id: Some(Uuid::new_v4()),
            message_type,
            payload,
            sender,
        }
    }

    /// Create a response envelope with matching correlation ID
    pub fn response(
        sender: NodeId,
        payload: Bytes,
        message_type: String,
        correlation_id: Uuid,
    ) -> Self {
        Self {
            correlation_id: Some(correlation_id),
            message_type,
            payload,
            sender,
        }
    }
}
