use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Isolation error.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Key generation failed.
    #[error("key generation failed: {0}")]
    KeyGeneration(String),

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
