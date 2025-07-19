//! Types for the PubSub service

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use thiserror::Error;
use uuid::Uuid;

use crate::foundation::types::ConsensusGroupId;
use proven_topology::NodeId;

/// Message type for PubSub
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PubSubMessageType {
    /// Regular publish message
    Publish,
    /// Request expecting response
    Request,
    /// Response to a request
    Response,
    /// Control message (interest updates, etc)
    Control,
}

/// Internal network message for PubSub
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubNetworkMessage {
    /// Unique message ID
    pub id: Uuid,
    /// Subject the message is for
    pub subject: String,
    /// Message payload
    pub payload: Bytes,
    /// Optional headers
    pub headers: Vec<(String, String)>,
    /// Timestamp
    pub timestamp: SystemTime,
    /// Source node
    pub source: NodeId,
    /// Message type
    pub msg_type: PubSubMessageType,
    /// Reply subject for requests
    pub reply_to: Option<String>,
    /// Correlation ID for request/response
    pub correlation_id: Option<Uuid>,
}

/// Interest update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestUpdateMessage {
    /// Node ID
    pub node_id: NodeId,
    /// Interests (subject patterns)
    pub interests: Vec<String>,
    /// Timestamp
    pub timestamp: SystemTime,
}

/// Result type for PubSub operations
pub type PubSubResult<T> = Result<T, PubSubError>;

/// Errors that can occur in PubSub operations
#[derive(Error, Debug)]
pub enum PubSubError {
    /// Network-related error
    #[error("Network error: {0}")]
    Network(String),

    /// Invalid subject pattern
    #[error("Invalid subject pattern: {0}")]
    InvalidSubject(String),

    /// Subscription not found
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),

    /// Request timed out
    #[error("Request timeout")]
    RequestTimeout,

    /// No responders available for request
    #[error("No responders available for subject: {0}")]
    NoResponders(String),

    /// Message too large
    #[error("Message too large: {0} bytes exceeds limit of {1} bytes")]
    MessageTooLarge(usize, usize),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Delivery mode for messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryMode {
    /// Best effort delivery (fire and forget)
    BestEffort,
    /// At least once delivery with acknowledgment
    AtLeastOnce,
    /// Persist to consensus stream
    Persistent,
}

/// Subscription configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription ID
    pub id: String,
    /// Subject pattern to subscribe to
    pub subject_pattern: String,
    /// Subscriber node ID
    pub node_id: NodeId,
    /// Whether this subscription should persist messages
    pub persist: bool,
    /// Optional group name for load balancing
    pub queue_group: Option<String>,
    /// Delivery mode
    pub delivery_mode: DeliveryMode,
    /// Created timestamp
    pub created_at: SystemTime,
}

/// Request for PubSub operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubRequest {
    /// Publish a message
    Publish {
        subject: String,
        payload: Bytes,
        headers: Vec<(String, String)>,
    },
    /// Send a request and wait for response
    Request {
        subject: String,
        payload: Bytes,
        headers: Vec<(String, String)>,
        timeout: Duration,
    },
    /// Subscribe to a subject pattern
    Subscribe {
        subject_pattern: String,
        queue_group: Option<String>,
        delivery_mode: DeliveryMode,
        persist: bool,
    },
    /// Unsubscribe from a subscription
    Unsubscribe { subscription_id: String },
    /// Update interest set (used for peer coordination)
    UpdateInterest {
        node_id: NodeId,
        interests: Vec<String>,
    },
}

/// Response from PubSub operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubResponse {
    /// Publish acknowledged
    Published { message_id: Uuid },
    /// Request response
    Response {
        payload: Bytes,
        headers: Vec<(String, String)>,
    },
    /// Subscription created
    Subscribed { subscription_id: String },
    /// Unsubscribed
    Unsubscribed,
    /// Interest updated
    InterestUpdated,
    /// Error response
    Error { message: String },
}

/// Statistics for PubSub operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PubSubStats {
    /// Total messages published
    pub messages_published: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Total subscriptions
    pub active_subscriptions: u64,
    /// Messages persisted to streams
    pub messages_persisted: u64,
    /// Request timeouts
    pub request_timeouts: u64,
    /// Average request latency in microseconds
    pub avg_request_latency_us: u64,
}

/// Stream mapping from global consensus
#[derive(Debug, Clone)]
pub struct StreamMapping {
    /// Stream ID
    pub stream_id: String,
    /// Subject pattern to match
    pub subject_pattern: String,
    /// Group ID that manages this stream
    pub group_id: ConsensusGroupId,
    /// Whether to auto-publish matching messages
    pub auto_publish: bool,
    /// Priority for conflicting patterns (higher wins)
    pub priority: i32,
}

/// Trait for global consensus operations
#[async_trait::async_trait]
pub trait GlobalConsensusHandle: Send + Sync {
    /// Get all stream mappings from global consensus
    async fn get_stream_mappings(&self) -> Result<Vec<StreamMapping>, Box<dyn std::error::Error>>;

    /// Subscribe to stream mapping updates
    async fn subscribe_to_mapping_updates(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Trait for group consensus operations
#[async_trait::async_trait]
pub trait GroupConsensusHandle: Send + Sync {
    /// Publish message to stream through group consensus
    async fn publish_to_stream(
        &self,
        group_id: ConsensusGroupId,
        stream_id: String,
        message: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
