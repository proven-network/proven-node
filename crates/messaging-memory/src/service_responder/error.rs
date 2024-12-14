use std::fmt::Debug;

use proven_messaging::service_responder::ServiceResponderError;
use thiserror::Error;

/// Errors that can occur in a service responder.
#[derive(Debug, Error)]
pub enum Error {
    /// Any error.
    #[error("Any")]
    Any,
}

impl ServiceResponderError for Error {}
