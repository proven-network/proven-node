use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Consumer error.
    #[error("error starting consumer")]
    Consumer,

    /// Consumer error.
    #[error("error starting service")]
    Service,

    /// An error occured while subscribing to a subject.
    #[error(transparent)]
    Subject(#[from] crate::subject::Error),

    /// An error occurred while handling a subscription.
    #[error(transparent)]
    SubscriptionHandler(#[from] super::subscription_handler::Error),
}

impl StreamError for Error {}
