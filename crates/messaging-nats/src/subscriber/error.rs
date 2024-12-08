use std::convert::Infallible;

use proven_messaging::subscription::SubscriptionError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error, Clone)]
pub enum Error<DE = Infallible, SE = Infallible>
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

impl<DE, SE> SubscriptionError for Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
}
