//! Error types for the messaging-consensus system.

use proven_messaging::{
    service_responder::ServiceResponderError, stream::StreamError, subject::SubjectError,
    subscription::SubscriptionError, subscription_responder::SubscriptionResponderError,
};

/// Messaging-specific error wrapper for consensus errors.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct MessagingConsensusError(#[from] pub proven_consensus::ConsensusError);

impl StreamError for MessagingConsensusError {}
impl SubjectError for MessagingConsensusError {}
impl ServiceResponderError for MessagingConsensusError {}
impl SubscriptionError for MessagingConsensusError {}
impl SubscriptionResponderError for MessagingConsensusError {}

// Re-export the underlying consensus error for convenience
pub use proven_consensus::ConsensusError;
pub use proven_consensus::error::ConsensusResult;
