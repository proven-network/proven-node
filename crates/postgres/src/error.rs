use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Failed to initialize database.
    #[error("failed to initialize database")]
    InitDb,

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// An error occurred in the isolation system.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Failed to parse output.
    #[error("failed to parse output")]
    OutputParse,

    /// Failed to vacuum.
    #[error("vacuum failed")]
    VacuumFailed,
}
