use proven_messaging::subject::SubjectError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// The subject name is invalid
    #[error("invalid subject name - must not contain '.', '*', or '>'")]
    InvalidSubjectPartial,

    /// Publish error.
    #[error("failed to publish: {0}")]
    Publish(async_nats::client::PublishErrorKind),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Error making subscription.
    #[error(transparent)]
    SubscriptionError(#[from] crate::subscription::Error),
}

impl SubjectError for Error {}
