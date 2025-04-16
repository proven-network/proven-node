use thiserror::Error;

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
    Io(&'static str, #[source] std::io::Error),

    /// Isolation error occurred.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// JSON error occurred.
    #[error("JSON error: {0}")]
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
}
