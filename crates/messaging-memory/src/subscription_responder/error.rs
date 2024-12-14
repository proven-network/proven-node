use std::fmt::Debug;

use proven_messaging::subscription_responder::SubscriptionResponderError;
use thiserror::Error;

/// Errors that can occur in a service responder.
#[derive(Debug, Error)]
pub enum Error {
    /// Any error.
    #[error("Any")]
    Any,
}

impl SubscriptionResponderError for Error {}
