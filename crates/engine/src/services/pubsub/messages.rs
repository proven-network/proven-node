//! PubSub-specific message types

use bytes::Bytes;
use proven_network::ServiceMessage;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// PubSub service message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubServiceMessage {
    /// Regular publish message
    Publish {
        /// Unique message ID
        id: Uuid,
        /// Subject the message was published to
        subject: String,
        /// Message payload
        payload: Bytes,
        /// Optional headers
        headers: Vec<(String, String)>,
        /// Timestamp when created
        timestamp: SystemTime,
        /// Source node
        source: NodeId,
        /// Optional reply subject for request-response
        reply_to: Option<String>,
        /// Optional correlation ID
        correlation_id: Option<Uuid>,
    },
    /// Request expecting a response
    Request {
        /// Unique message ID
        id: Uuid,
        /// Subject the message was published to
        subject: String,
        /// Message payload
        payload: Bytes,
        /// Optional headers
        headers: Vec<(String, String)>,
        /// Timestamp when created
        timestamp: SystemTime,
        /// Source node
        source: NodeId,
        /// Reply subject for response
        reply_to: String,
        /// Correlation ID
        correlation_id: Uuid,
    },
    /// Response to a request
    Response {
        /// Unique message ID
        id: Uuid,
        /// Original subject
        subject: String,
        /// Message payload
        payload: Bytes,
        /// Optional headers
        headers: Vec<(String, String)>,
        /// Timestamp when created
        timestamp: SystemTime,
        /// Source node
        source: NodeId,
        /// Correlation ID from request
        correlation_id: Uuid,
    },
    /// Interest update message for peer coordination
    InterestUpdate {
        /// Node ID
        node_id: NodeId,
        /// Subject patterns this node is interested in
        interests: Vec<String>,
        /// Timestamp
        timestamp: SystemTime,
    },
}

/// PubSub service response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubServiceResponse {
    /// Acknowledgment of publish
    PublishAck {
        /// Message ID that was published
        message_id: Uuid,
        /// Number of subscribers who received it
        subscriber_count: usize,
    },
    /// Response data
    Response {
        /// Response payload
        payload: Bytes,
        /// Optional headers
        headers: Vec<(String, String)>,
    },
    /// Interest update acknowledgment
    InterestUpdateAck,
}

impl ServiceMessage for PubSubServiceMessage {
    type Response = PubSubServiceResponse;

    fn service_id() -> &'static str {
        "pubsub"
    }
}
