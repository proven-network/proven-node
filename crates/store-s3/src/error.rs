use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Bad content length.
    #[error("Bad content length: {0}")]
    BadContentLength(i64),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// S3 error.
    #[error(transparent)]
    S3(#[from] aws_sdk_s3::Error),
}

impl StoreError for Error {}
