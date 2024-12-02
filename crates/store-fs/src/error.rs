use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// IO operation failed.
    #[error("{0}: {1}")]
    IoError(&'static str, #[source] std::io::Error),
}

impl StoreError for Error {}
