use proven_messaging::SubscriberError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Subscriber error")]
pub struct Error;

impl SubscriberError for Error {}
