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

    /// IO operation failed.
    #[error("{0}: {1}")]
    IoError(&'static str, #[source] std::io::Error),

    /// JSON encode error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Process exited with non-zero.
    #[error("exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    /// Failed to parse output.
    #[error("failed to parse output")]
    OutputParse,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),

    /// Failed to send signal to dnscrypt-proxy.
    #[error(transparent)]
    Signal(#[from] nix::Error),
}
