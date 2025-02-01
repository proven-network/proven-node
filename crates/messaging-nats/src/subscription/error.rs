use proven_messaging::subscription::SubscriptionError;
use thiserror::Error;

/// Error type for NATS operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Subscribe error.
    #[error("failed to subscribe")]
    Subscribe,

    /// Unsubscribe error.
    #[error("failed to unsubscribe")]
    Unsubscribe,
}

impl SubscriptionError for Error {}
