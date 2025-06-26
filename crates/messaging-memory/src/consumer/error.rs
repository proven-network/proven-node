use std::fmt::Debug;

use proven_messaging::consumer::ConsumerError;
use thiserror::Error;

/// Errors that can occur in a consumer.
#[derive(Debug, Error)]
pub enum Error {
    /// Already running.
    #[error("Already running")]
    AlreadyRunning,

    /// Handler error.
    #[error("Handler error")]
    Handler,
}

impl ConsumerError for Error {}
