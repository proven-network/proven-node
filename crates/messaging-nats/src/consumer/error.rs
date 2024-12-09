use std::fmt::Debug;

use proven_messaging::consumer::ConsumerError;
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    /// An error occurred while pulling messages from the stream.
    #[error("An error occurred while pulling messages from the stream.")]
    Something,

    /// An error occurred while pulling messages from the stream.
    #[error("An error occurred while pulling messages from the stream.")]
    Else(T),
    // / An error occurred while pulling messages from the stream.
    // #[error(transparent)]
    // Stream(#[from] Box<crate::stream::Error<T>>),
}

impl<T> ConsumerError for Error<T> where T: Clone + Debug + Send + Sync + 'static {}
