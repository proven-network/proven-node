//! Error types for the messaging-engine system.

use proven_messaging::{
    service_responder::ServiceResponderError, stream::StreamError, subject::SubjectError,
    subscription::SubscriptionError, subscription_responder::SubscriptionResponderError,
};

/// Messaging-specific error for engine-based messaging.
#[derive(Debug, thiserror::Error)]
pub enum MessagingEngineError {
    /// Engine error
    #[error("Engine error: {0}")]
    Engine(String),

    /// Stream not found
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Subject error
    #[error("Subject error: {0}")]
    Subject(String),
}

impl StreamError for MessagingEngineError {}
impl SubjectError for MessagingEngineError {}
impl ServiceResponderError for MessagingEngineError {}
impl SubscriptionError for MessagingEngineError {}
impl SubscriptionResponderError for MessagingEngineError {}
