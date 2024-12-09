use proven_messaging::{
    subscription::SubscriptionError, subscription_handler::SubscriptionHandlerError,
};
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Subscriber error")]
pub enum Error<HE>
where
    HE: SubscriptionHandlerError,
{
    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(HE),
}

impl<HE> SubscriptionError for Error<HE> where HE: SubscriptionHandlerError {}
