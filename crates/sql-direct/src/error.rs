use deno_error::JsError;
use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error, JsError)]
pub enum Error {
    /// Libsql errors.
    #[class(generic)]
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl SqlStoreError for Error {}
