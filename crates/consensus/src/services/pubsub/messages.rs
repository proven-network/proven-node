//! PubSub-specific message types

use bytes::Bytes;
use proven_network::message::HandledMessage;
use proven_network::namespace::MessageType;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// PubSub message that can be sent over the network
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubNetworkMessage {
    /// Unique message ID
    pub id: Uuid,
    /// Subject the message was published to
    pub subject: String,
    /// Message payload
    pub payload: Bytes,
    /// Optional headers
    pub headers: Vec<(String, String)>,
    /// Timestamp when created
    pub timestamp: SystemTime,
    /// Source node
    pub source: NodeId,
    /// Message type
    pub msg_type: PubSubMessageType,
    /// Optional reply subject for request-response
    pub reply_to: Option<String>,
    /// Optional correlation ID
    pub correlation_id: Option<Uuid>,
}

/// Type of PubSub message
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PubSubMessageType {
    /// Regular publish message
    Publish,
    /// Request expecting a response
    Request,
    /// Response to a request
    Response,
    /// System control message
    Control,
}

/// Interest update message for peer coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestUpdateMessage {
    /// Node ID
    pub node_id: NodeId,
    /// Subject patterns this node is interested in
    pub interests: Vec<String>,
    /// Timestamp
    pub timestamp: SystemTime,
}

// Implement MessageType for PubSub types
impl MessageType for PubSubNetworkMessage {
    fn message_type(&self) -> &'static str {
        match self.msg_type {
            PubSubMessageType::Publish => "pubsub_publish",
            PubSubMessageType::Request => "pubsub_request",
            PubSubMessageType::Response => "pubsub_response",
            PubSubMessageType::Control => "pubsub_control",
        }
    }
}

impl MessageType for InterestUpdateMessage {
    fn message_type(&self) -> &'static str {
        "pubsub_interest_update"
    }
}

// Note: We could add HandledMessage implementations if we want request/response patterns
// For now, PubSub messages are fire-and-forget or use their own correlation
