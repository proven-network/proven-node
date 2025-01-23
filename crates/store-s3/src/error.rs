use deno_error::JsError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error, JsError)]
pub enum Error {
    /// Bad content length.
    #[class(generic)]
    #[error("Bad content length: {0}")]
    BadContentLength(i64),

    /// Deserialization error.
    #[class(generic)]
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// IO operation failed.
    #[class(generic)]
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// S3 error.
    #[class(generic)]
    #[error(transparent)]
    S3(#[from] aws_sdk_s3::Error),

    /// Serialization error.
    #[class(generic)]
    #[error("serialization error: {0}")]
    Serialize(String),
}

impl StoreError for Error {}
