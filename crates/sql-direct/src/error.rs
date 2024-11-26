use proven_sql::SqlStoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl SqlStoreError for Error {}
