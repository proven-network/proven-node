use std::fmt::Debug;

use proven_messaging::subscription_handler::SubscriptionHandlerError;
use thiserror::Error;

/// Error type for memory stream subscription handlers.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while sending data.
    #[error("An error occurred while sending data")]
    Send,
}

impl SubscriptionHandlerError for Error {}
