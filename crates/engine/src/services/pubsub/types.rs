//! Types for the PubSub service

use std::time::SystemTime;

use bytes::Bytes;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::foundation::types::{SubjectError, SubjectPattern};

/// Message type for PubSub
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PubSubMessageType {
    /// Regular publish message
    Publish,
    /// Control message (subscribe, unsubscribe, interest updates, etc)
    Control,
}

/// Internal network message for PubSub
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubNetworkMessage {
    /// Unique message ID
    pub id: Uuid,
    /// Message payload
    pub payload: Bytes,
    /// Headers (including subject)
    pub headers: Vec<(String, String)>,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Source node
    pub source: NodeId,
    /// Message type
    pub msg_type: PubSubMessageType,
}

impl PubSubNetworkMessage {
    /// Get the subject from headers
    pub fn subject(&self) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == "subject")
            .map(|(_, v)| v.as_str())
    }
}

/// Errors that can occur in PubSub operations
#[derive(Error, Debug)]
pub enum PubSubError {
    /// Network-related error
    #[error("Network error: {0}")]
    Network(String),

    /// Invalid subject pattern
    #[error("Invalid subject: {0}")]
    InvalidSubject(#[from] SubjectError),

    /// Subscription not found
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),

    /// Maximum subscriptions exceeded
    #[error("Maximum subscriptions exceeded: {0}")]
    MaxSubscriptionsExceeded(usize),

    /// Message too large
    #[error("Message too large: {0} bytes exceeds limit of {1} bytes")]
    MessageTooLarge(usize, usize),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Subscription configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription ID
    pub id: String,
    /// Subject pattern to subscribe to
    pub subject_pattern: SubjectPattern,
    /// Subscriber node ID
    pub node_id: NodeId,
    /// Optional group name for load balancing
    pub queue_group: Option<String>,
    /// Created timestamp
    pub created_at: SystemTime,
}
