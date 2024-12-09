use std::fmt::Debug;

use proven_messaging::{service::ServiceError, service_handler::ServiceHandlerError};
use thiserror::Error;

/// Errors that can occur in a service.
#[derive(Debug, Error)]
pub enum Error<HE>
where
    HE: ServiceHandlerError,
{
    /// Handler error.
    #[error("Handler error: {0}")]
    Handler(HE),
}

impl<HE> ServiceError for Error<HE> where HE: ServiceHandlerError {}
