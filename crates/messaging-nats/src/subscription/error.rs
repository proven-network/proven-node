use std::error::Error as StdError;

use proven_messaging::subscription::SubscriptionError;
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

    /// Serialization error.
    #[error(transparent)]
    Serialize(SE),

    /// Subscribe error.
    #[error("Failed to subscribe")]
    Subscribe,

    /// Unsubscribe error.
    #[error("Failed to unsubscribe")]
    Unsubscribe,
}

impl<DE, SE> SubscriptionError for Error<DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
{
}
