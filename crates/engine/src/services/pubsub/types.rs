//! Types for the PubSub service

use std::time::SystemTime;

use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::foundation::types::{SubjectError, SubjectPattern};

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

    /// No responders for the given subject
    #[error("No responders for subject: {0}")]
    NoResponders(String),
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
