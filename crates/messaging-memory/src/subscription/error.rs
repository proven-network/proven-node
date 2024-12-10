use proven_messaging::subscription::SubscriptionError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Subscriber error")]
pub enum Error {
    /// Handler error.
    #[error("Handler error:")]
    Handler,
}

impl SubscriptionError for Error {}
