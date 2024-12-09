use std::fmt::Debug;

use proven_messaging::subscription_handler::SubscriptionHandlerError;
use proven_messaging::Message;
use thiserror::Error;

/// Error type for memory stream subscription handlers.
#[derive(Debug, Error)]
pub enum Error<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    /// An error occurred while sending data.
    #[error("An error occurred while sending data: {0}")]
    Send(#[from] tokio::sync::mpsc::error::SendError<Message<T>>),
}

impl<T> SubscriptionHandlerError for Error<T> where T: Clone + Debug + Send + Sync + 'static {}
