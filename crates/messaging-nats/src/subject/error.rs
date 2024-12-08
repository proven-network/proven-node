use proven_messaging::SubjectError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error, Clone)]
pub enum Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
    /// Deserialization error.
    #[error(transparent)]
    Deserialize(DE),

    /// The subject name is invalid
    #[error("invalid subject name - must not contain '.', '*', or '>'")]
    InvalidSubjectPartial,

    /// Publish error.
    #[error("Failed to publish: {0}")]
    Publish(async_nats::client::PublishErrorKind),

    /// Serialization error.
    #[error(transparent)]
    Serialize(SE),
}

impl<DE, SE> SubjectError for Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
}
