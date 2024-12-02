use crate::SqlType;

use std::sync::Arc;

use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Incorrect SQL type for request.
    #[error("Incorrect SQL type. Expected {0}, got {1}")]
    IncorrectSqlType(SqlType, SqlType),

    /// Invalid column count.
    #[error("Invalid column count")]
    InvalidColumnCount,

    /// Libsql error.
    #[error(transparent)]
    Libsql(Arc<libsql::Error>),

    /// Used reserved table prefix.
    #[error("Cannot use reserved table prefix")]
    UsedReservedTablePrefix,
}

impl From<libsql::Error> for Error {
    fn from(error: libsql::Error) -> Self {
        Self::Libsql(Arc::new(error))
    }
}
