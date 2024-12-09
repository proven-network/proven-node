use proven_messaging::consumer::ConsumerError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
#[error("Consumer error")]
pub struct Error;

impl ConsumerError for Error {}
