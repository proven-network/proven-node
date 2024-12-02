use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Libsql errors.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl SqlStoreError for Error {}
