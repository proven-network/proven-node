use std::convert::Infallible;
use std::error::Error as StdError;

use proven_messaging::subscription_handler::SubscriptionHandlerError;
use thiserror::Error;

/// Error type for memory stream subscription handlers.
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

    /// An error occurred.
    // TODO: This should be more specific.
    #[error("An error occurred")]
    Other,
}

impl<DE, SE> SubscriptionHandlerError for Error<DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
{
}
