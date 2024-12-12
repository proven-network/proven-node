use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Consumer error.
    #[error("error starting consumer")]
    Consumer,

    /// Did an operation on a seq that doesn't exist yet.
    #[error("invalid seq. requested: {0}, current: {1}")]
    InvalidSeq(usize, usize),

    /// An error occured while subscribing to a subject.
    #[error(transparent)]
    Subject(#[from] crate::subject::Error),

    /// An error occurred while handling a subscription.
    #[error(transparent)]
    SubscriptionHandler(#[from] super::subscription_handler::Error),
}

impl StreamError for Error {}
