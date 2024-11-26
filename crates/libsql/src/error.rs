use crate::SqlType;

use std::sync::Arc;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Incorrect SQL type. Expected {0}, got {1}")]
    IncorrectSqlType(SqlType, SqlType),

    #[error(transparent)]
    Libsql(Arc<libsql::Error>),
}

impl From<libsql::Error> for Error {
    fn from(error: libsql::Error) -> Self {
        Error::Libsql(Arc::new(error))
    }
}
