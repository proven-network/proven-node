//! PubSub message types
//!
//! Defines the message types used by the PubSub system for communication
//! between nodes about subject interests and message publishing.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use uuid::Uuid;

use proven_topology::NodeId;

/// PubSub protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubMessage {
    /// Subscribe to a subject pattern
    Subscribe {
        /// The subject pattern to subscribe to (supports wildcards)
        subject: String,
        /// Unique subscription ID
        subscription_id: String,
        /// Node making the subscription
        node_id: NodeId,
    },

    /// Unsubscribe from a subject pattern
    Unsubscribe {
        /// The subject pattern to unsubscribe from
        subject: String,
        /// Subscription ID to remove
        subscription_id: String,
        /// Node removing the subscription
        node_id: NodeId,
    },

    /// Publish a message to a subject
    Publish {
        /// The subject to publish to
        subject: String,
        /// Message payload
        payload: Bytes,
        /// Optional reply-to subject for request-response pattern
        reply_to: Option<String>,
        /// Message ID for deduplication
        message_id: Uuid,
    },

    /// Announce interest changes to peers (expects acknowledgment)
    InterestUpdate {
        /// Complete set of subject patterns this node is interested in
        interests: HashSet<String>,
        /// Node announcing its interests
        node_id: NodeId,
    },

    /// Acknowledgment of interest update
    InterestUpdateAck {
        /// Node acknowledging the update
        node_id: NodeId,
        /// Whether the update was successful
        success: bool,
        /// Error message if not successful
        error: Option<String>,
    },

    /// Request-response pattern request
    Request {
        /// The subject to send request to
        subject: String,
        /// Request payload
        payload: Bytes,
        /// Reply subject for response
        reply_to: String,
        /// Request ID for correlation
        request_id: Uuid,
    },

    /// Response to a request
    Response {
        /// Original request ID
        request_id: Uuid,
        /// Response payload
        payload: Bytes,
        /// Responder node ID
        responder: NodeId,
    },
}

/// Wrapper for PubSub requests that expect responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubRequest {
    /// The actual message
    pub message: PubSubMessage,
    /// Correlation ID for tracking responses
    pub correlation_id: Uuid,
}

/// Wrapper for PubSub responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubResponse {
    /// The response message
    pub message: PubSubMessage,
    /// Original correlation ID
    pub correlation_id: Uuid,
    /// Whether the operation was successful
    pub success: bool,
    /// Error message if not successful
    pub error: Option<String>,
}

/// Interest update acknowledgment details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestUpdateAck {
    /// Node acknowledging the update
    pub node_id: NodeId,
    /// Whether the update was successful
    pub success: bool,
    /// Error message if not successful
    pub error: Option<String>,
}

impl PubSubMessage {
    /// Check if this message requires a response
    pub fn requires_response(&self) -> bool {
        matches!(
            self,
            PubSubMessage::Subscribe { .. }
                | PubSubMessage::Unsubscribe { .. }
                | PubSubMessage::InterestUpdate { .. }
                | PubSubMessage::Request { .. }
        )
    }

    /// Get the subject for routing purposes
    pub fn subject(&self) -> Option<&str> {
        match self {
            PubSubMessage::Subscribe { subject, .. } => Some(subject),
            PubSubMessage::Unsubscribe { subject, .. } => Some(subject),
            PubSubMessage::Publish { subject, .. } => Some(subject),
            PubSubMessage::Request { subject, .. } => Some(subject),
            _ => None,
        }
    }
}
