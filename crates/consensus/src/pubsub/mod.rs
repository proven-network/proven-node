//! PubSub implementation for high-performance messaging
//!
//! This module provides a lightweight publish-subscribe system that operates
//! independently of the consensus layer. It enables fast, direct peer-to-peer
//! message forwarding based on subject interests.

mod interest;
mod manager;
mod messages;
mod router;
mod stream_bridge;
mod subject;

#[cfg(test)]
mod tests;

pub use manager::{PubSubManager, Subscription};
pub use messages::{PubSubMessage, PubSubRequest, PubSubResponse};
pub use stream_bridge::StreamBridge;
pub use subject::{
    SubjectRouter, subject_matches_pattern, validate_subject, validate_subject_pattern,
};

// Re-export error types
/// Result type for PubSub operations
pub type PubSubResult<T> = Result<T, PubSubError>;

use thiserror::Error;

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

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}
