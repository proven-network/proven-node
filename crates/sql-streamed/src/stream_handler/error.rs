use thiserror::Error;

/// Result type for the SQL stream handler.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in a SQL stream handler.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Libsql error.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}
