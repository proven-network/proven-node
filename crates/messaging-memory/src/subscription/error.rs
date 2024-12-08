use proven_messaging::subscription::SubscriptionError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Subscriber error")]
pub struct Error;

impl SubscriptionError for Error {}
