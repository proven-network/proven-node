use proven_messaging::subscription::SubscriptionError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Deserialization error.
    #[error(transparent)]
    Deserialize(#[from] ciborium::de::Error<std::io::Error>),

    /// Serialization error.
    #[error(transparent)]
    Serialize(#[from] ciborium::ser::Error<std::io::Error>),

    /// Subscribe error.
    #[error("Failed to subscribe")]
    Subscribe,

    /// Unsubscribe error.
    #[error("Failed to unsubscribe")]
    Unsubscribe,
}

impl SubscriptionError for Error {}
