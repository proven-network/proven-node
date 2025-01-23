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

    /// Serialization error.
    #[class(generic)]
    #[error("serialization error: {0}")]
    Serialize(String),
}

impl StoreError for Error {}
