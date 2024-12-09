use proven_messaging::subject::SubjectError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Deserialization error.
    #[error(transparent)]
    Deserialize(#[from] ciborium::de::Error<std::io::Error>),

    /// The subject name is invalid
    #[error("invalid subject name - must not contain '.', '*', or '>'")]
    InvalidSubjectPartial,

    /// Publish error.
    #[error("Failed to publish: {0}")]
    Publish(async_nats::client::PublishErrorKind),

    /// Serialization error.
    #[error(transparent)]
    Serialize(#[from] ciborium::ser::Error<std::io::Error>),

    /// Error making subscription.
    #[error(transparent)]
    SubscriptionError(#[from] crate::subscription::Error),
}

impl SubjectError for Error {}
