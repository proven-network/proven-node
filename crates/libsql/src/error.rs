use crate::SqlType;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Copy of `libsql::Error` with some adjustments for serialization.
#[derive(Clone, Debug, Deserialize, Error, Serialize)]
pub enum LibSqlError {
    #[error("Failed to connect to database: `{0}`")]
    ConnectionFailed(String),

    #[error("SQLite failure: `{1}`")]
    SqliteFailure(std::ffi::c_int, String),

    #[error("Null value")]
    NullValue, // Not in rusqlite

    #[error("API misuse: `{0}`")]
    Misuse(String), // Not in rusqlite

    #[error("Execute returned rows")]
    ExecuteReturnedRows,

    #[error("Query returned no rows")]
    QueryReturnedNoRows,

    #[error("Invalid column name: `{0}`")]
    InvalidColumnName(String),

    #[error("SQL conversion failure: `{0}`")]
    ToSqlConversionFailure(String),

    #[error("Sync is not supported in databases opened in {0} mode.")]
    SyncNotSupported(String), // Not in rusqlite

    #[error("Loading extension is only supported in local databases.")]
    LoadExtensionNotSupported, // Not in rusqlite

    #[error("Column not found: {0}")]
    ColumnNotFound(i32), // Not in rusqlite

    #[error("Hrana: `{0}`")]
    Hrana(String), // Not in rusqlite

    #[error("Write delegation: `{0}`")]
    WriteDelegation(String), // Not in rusqlite

    #[error("bincode: `{0}`")]
    Bincode(String),

    #[error("invalid column index")]
    InvalidColumnIndex,

    #[error("invalid column type")]
    InvalidColumnType,

    #[error("syntax error around L{0}:{1}: `{2}`")]
    Sqlite3SyntaxError(u64, usize, String),

    #[error("unsupported statement")]
    Sqlite3UnsupportedStatement,

    #[error("sqlite3 parser error: `{0}`")]
    Sqlite3ParserError(String),

    #[error("Remote SQlite failure: `{0}:{1}:{2}`")]
    RemoteSqliteFailure(i32, i32, String),

    #[error("replication error: {0}")]
    Replication(String),

    #[error("path has invalid UTF-8")]
    InvalidUTF8Path,

    #[error("freeze is not supported in {0} mode.")]
    FreezeNotSupported(String),

    #[error("connection has reached an invalid state, started with {0}")]
    InvalidParserState(String),

    #[error("TLS error: {0}")]
    InvalidTlsConfiguration(String),

    #[error("Transactional batch error: {0}")]
    TransactionalBatchError(String),

    #[error("Invalid blob size, expected {0}")]
    InvalidBlobSize(usize),

    /// Added due to non-exhaustive enum.
    #[error("{0}")]
    OtherLibsqlError(String),
}

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Deserialize, Error, Serialize)]
pub enum Error {
    /// Incorrect SQL type for request.
    #[error("Incorrect SQL type. Expected {0}, got {1}")]
    IncorrectSqlType(SqlType, SqlType),

    /// Invalid column count.
    #[error("Invalid column count")]
    InvalidColumnCount,

    /// Libsql error.
    #[error(transparent)]
    Libsql(LibSqlError),

    /// Used reserved table prefix.
    #[error("Cannot use reserved table prefix")]
    UsedReservedTablePrefix,
}

impl From<libsql::Error> for Error {
    fn from(error: libsql::Error) -> Self {
        Self::Libsql(match error {
            libsql::Error::ConnectionFailed(e) => LibSqlError::ConnectionFailed(e),
            libsql::Error::SqliteFailure(i, e) => LibSqlError::SqliteFailure(i, e),
            libsql::Error::NullValue => LibSqlError::NullValue,
            libsql::Error::Misuse(e) => LibSqlError::Misuse(e),
            libsql::Error::ExecuteReturnedRows => LibSqlError::ExecuteReturnedRows,
            libsql::Error::QueryReturnedNoRows => LibSqlError::QueryReturnedNoRows,
            libsql::Error::InvalidColumnName(e) => LibSqlError::InvalidColumnName(e),
            libsql::Error::ToSqlConversionFailure(e) => {
                LibSqlError::ToSqlConversionFailure(format!("{e}"))
            }
            libsql::Error::SyncNotSupported(e) => LibSqlError::SyncNotSupported(e),
            libsql::Error::LoadExtensionNotSupported => LibSqlError::LoadExtensionNotSupported,
            libsql::Error::ColumnNotFound(e) => LibSqlError::ColumnNotFound(e),
            libsql::Error::Hrana(e) => LibSqlError::Hrana(format!("{e}")),
            libsql::Error::WriteDelegation(e) => LibSqlError::WriteDelegation(format!("{e}")),
            libsql::Error::Bincode(e) => LibSqlError::Bincode(format!("{e}")),
            libsql::Error::InvalidColumnIndex => LibSqlError::InvalidColumnIndex,
            libsql::Error::InvalidColumnType => LibSqlError::InvalidColumnType,
            libsql::Error::Sqlite3SyntaxError(l, u, e) => LibSqlError::Sqlite3SyntaxError(l, u, e),
            libsql::Error::Sqlite3UnsupportedStatement => LibSqlError::Sqlite3UnsupportedStatement,
            libsql::Error::Sqlite3ParserError(e) => LibSqlError::Sqlite3ParserError(format!("{e}")),
            libsql::Error::RemoteSqliteFailure(i, j, e) => {
                LibSqlError::RemoteSqliteFailure(i, j, e)
            }
            libsql::Error::Replication(e) => LibSqlError::Replication(format!("{e}")),
            libsql::Error::InvalidUTF8Path => LibSqlError::InvalidUTF8Path,
            libsql::Error::FreezeNotSupported(e) => LibSqlError::FreezeNotSupported(e),
            libsql::Error::InvalidParserState(e) => LibSqlError::InvalidParserState(e),
            libsql::Error::InvalidTlsConfiguration(e) => {
                LibSqlError::InvalidTlsConfiguration(format!("{e}"))
            }
            libsql::Error::TransactionalBatchError(e) => LibSqlError::TransactionalBatchError(e),
            libsql::Error::InvalidBlobSize(e) => LibSqlError::InvalidBlobSize(e),
            e => LibSqlError::OtherLibsqlError(format!("{e}")),
        })
    }
}
