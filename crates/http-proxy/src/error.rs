use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Client setup failed.
    #[error("client setup failed: {0}")]
    ClientSetup(String),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),
}
