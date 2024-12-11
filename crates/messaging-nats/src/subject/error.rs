use std::error::Error as StdError;

use proven_messaging::subject::SubjectError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error)]
pub enum Error<DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
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

    /// Error making subscription.
    #[error(transparent)]
    SubscriptionError(#[from] crate::subscription::Error<DE, SE>),
}

impl<DE, SE> SubjectError for Error<DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
{
}
