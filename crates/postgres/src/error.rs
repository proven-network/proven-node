use proven_bootable::BootableError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error
where
    Self: Send,
{
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Database initialization failed.
    #[error("database initialization failed")]
    DatabaseInit,

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// An error occurred in the isolation system.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Vacuuming database failed.
    #[error("vacuuming database failed")]
    Vacuum,
}

impl BootableError for Error {}
