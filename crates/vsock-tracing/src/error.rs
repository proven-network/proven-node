use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// IO operation failed.
    #[error("{0}: {1}")]
    IoError(&'static str, #[source] std::io::Error),

    /// Could not set global default subscriber.
    #[error("could not set global default subscriber: {0}")]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),

    /// Tokio join error.
    #[error(transparent)]
    Tokio(#[from] tokio::task::JoinError),
}
