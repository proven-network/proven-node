use proven_messaging::subject::SubjectError;
use thiserror::Error;

/// An error that can occur when working with subjects.
#[derive(Debug, Error)]
pub enum Error {
    /// The subject name is invalid.
    #[error("invalid subject name - must not contain '.', '*', or '>'")]
    InvalidSubjectPartial,

    /// Error making subscription.
    #[error(transparent)]
    SubscriptionError(#[from] crate::subscription::Error),
}

impl SubjectError for Error {}
