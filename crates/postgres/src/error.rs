use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Bad PID.
    #[error("bad PID")]
    BadPid,

    /// Failed to write config file.
    #[error("failed to write config: {0}")]
    ConfigWrite(#[from] std::io::Error),

    /// Process exited with non-zero.
    #[error("exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    /// Failed to parse output.
    #[error("failed to parse output")]
    OutputParse,

    /// Failed to spawn process.
    #[error("failed to spawn: {0}")]
    Spawn(std::io::Error),

    /// Failed to vacuum.
    #[error("vacuum failed")]
    VacuumFailed,
}
