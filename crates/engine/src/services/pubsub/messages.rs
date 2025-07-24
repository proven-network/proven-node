//! PubSub-specific message types

use bytes::Bytes;
use proven_network::ServiceMessage;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

use crate::foundation::types::subject::{Subject, SubjectPattern};

/// Single message notification for forwarding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageNotification {
    /// Unique message ID
    pub id: Uuid,
    /// Subject the message was published to
    pub subject: Subject,
    /// Message payload
    pub payload: Bytes,
    /// Optional headers
    pub headers: Vec<(String, String)>,
    /// Timestamp when created
    pub timestamp: SystemTime,
    /// Source node
    pub source: NodeId,
}

/// PubSub service message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum PubSubServiceMessage {
    /// Notify peer nodes of messages (supports batching)
    Notify {
        /// Batch of messages to forward
        messages: Vec<MessageNotification>,
    },
    /// Register interest in subject patterns
    RegisterInterest {
        /// Subject patterns this node is interested in
        patterns: Vec<SubjectPattern>,
        /// Timestamp
        timestamp: SystemTime,
    },
    /// Unregister interest in subject patterns
    UnregisterInterest {
        /// Subject patterns to remove interest in
        patterns: Vec<SubjectPattern>,
        /// Timestamp
        timestamp: SystemTime,
    },
}

/// PubSub service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum PubSubServiceResponse {
    /// Acknowledgment
    Ack,
    /// Error occurred
    Error {
        /// Error message
        message: String,
    },
}

impl ServiceMessage for PubSubServiceMessage {
    type Response = PubSubServiceResponse;

    fn service_id() -> &'static str {
        "pubsub"
    }
}
