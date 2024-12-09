use std::fmt::Debug;

use proven_messaging::stream::StreamError;
use thiserror::Error;

/// Error type for memory stream operations.
#[derive(Debug, Error)]
pub enum Error<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Consumer error.
    #[error("error starting consumer")]
    Consumer,

    /// An error occured while subscribing to a subject.
    #[error(transparent)]
    Subject(#[from] crate::subject::Error),

    /// An error occurred while handling a subscription.
    #[error(transparent)]
    SubscriptionHandler(#[from] super::subscription_handler::Error<T>),
}

impl<T> StreamError for Error<T> where T: Clone + Debug + Send + Sync + 'static {}
