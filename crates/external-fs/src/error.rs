use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// External fs already started.
    #[error("external fs already started")]
    AlreadyStarted,

    /// IO operation failed.
    #[error("{0}: {1}")]
    IoError(&'static str, #[source] std::io::Error),

    /// Process exited with non-zero.
    #[error("external fs exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    /// Failed to parse external fs output.
    #[error("failed to parse external fs output")]
    OutputParse,

    /// Failed to spawn external fs.
    #[error("failed to spawn: {0}")]
    Spawn(#[from] std::io::Error),
}
