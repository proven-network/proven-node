use proven_messaging::SubjectError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Subject error")]
pub struct Error;

impl SubjectError for Error {}
