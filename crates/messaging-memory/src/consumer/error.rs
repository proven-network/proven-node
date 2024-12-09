use std::fmt::Debug;

use proven_messaging::{consumer::ConsumerError, consumer_handler::ConsumerHandlerError};
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error<HE>
where
    HE: ConsumerHandlerError,
{
    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(HE),
}

impl<HE> ConsumerError for Error<HE> where HE: ConsumerHandlerError {}
