use proven_messaging::SubscriberError;
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

    /// Serialization error.
    #[error(transparent)]
    Serialize(SE),

    /// Subscribe error.
    #[error("Failed to subscribe")]
    Subscribe,
}

impl<DE, SE> SubscriberError for Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
}
