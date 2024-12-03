use thiserror::Error;

/// Result type used by the enclave.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the enclave.
#[derive(Debug, Error)]
pub enum Error {
    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Could not set global default subscriber.
    #[error("could not set global default subscriber: {0}")]
    SetTracing(#[from] tracing::dispatcher::SetGlobalDefaultError),

    /// Tokio join error.
    #[error(transparent)]
    Tokio(#[from] tokio::task::JoinError),
}
