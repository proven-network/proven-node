use deno_error::JsError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error, JsError)]
pub enum Error {
    /// Deserialization error.
    #[class(generic)]
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// IO operation failed.
    #[class(generic)]
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Serialization error.
    #[class(generic)]
    #[error("serialization error: {0}")]
    Serialize(String),
}

impl StoreError for Error {}
