use std::fmt::Debug;

use proven_messaging::consumer::ConsumerError;
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error {
    /// Handler error.
    #[error("Handler error")]
    Handler,
}

impl ConsumerError for Error {}
