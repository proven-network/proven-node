use thiserror::Error;

/// The result type used by the host.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the host.
#[derive(Debug, Error)]
pub enum Error {
    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),
}
