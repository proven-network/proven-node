use std::fmt::Debug;

use proven_bootable::BootableError;
use proven_messaging::service::ServiceError;
use thiserror::Error;

/// Errors that can occur in a service.
#[derive(Debug, Error)]
pub enum Error {
    /// Already running.
    #[error("Already running")]
    AlreadyRunning,

    /// Handler error.
    #[error("Handler error")]
    Handler,
}

impl BootableError for Error {}
impl ServiceError for Error {}
