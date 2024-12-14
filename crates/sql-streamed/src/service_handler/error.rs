use thiserror::Error;

/// Errors that can occur in a SQL stream handler.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Libsql error.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}
