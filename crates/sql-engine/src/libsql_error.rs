//! `LibSQL` error types integrated into sql-engine.

use deno_error::{JsErrorClass, PropertyValue};
use std::borrow::Cow;
use thiserror::Error;

/// Statement types for SQL classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StatementType {
    /// DDL statements (CREATE TABLE, etc.)
    Migration,
    /// DML statements (INSERT, UPDATE, etc.)
    Mutation,
    /// Query statements (SELECT)
    Query,
    /// Unknown statement type
    Unknown,
}

impl std::fmt::Display for StatementType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Migration => write!(f, "Migration"),
            Self::Mutation => write!(f, "Mutation"),
            Self::Query => write!(f, "Query"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// LibSQL-specific errors
#[derive(Error, Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LibsqlError {
    /// Unknown error
    #[error("Unknown error: {0}")]
    Unknown(String),

    /// Must use file, not memory
    #[error("Must use file path, not :memory:")]
    MustUseFile,

    /// Used reserved table prefix
    #[error("Cannot use reserved table prefix")]
    UsedReservedTablePrefix,

    /// Schema name not allowed in migrations
    #[error("Schema names are not allowed in table migrations")]
    SchemaNameNotAllowed,

    /// Incorrect SQL type
    #[error("Incorrect SQL type: expected {0}, got {1}")]
    IncorrectSqlType(StatementType, StatementType),

    /// Backup in progress
    #[error("Backup is in progress")]
    BackupInProgress,

    /// `LibSQL` error
    #[error("LibSQL error: {0}")]
    Libsql(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(String),
}

impl JsErrorClass for LibsqlError {
    fn get_class(&self) -> Cow<'static, str> {
        match self {
            Self::Unknown(_) => Cow::Borrowed("LibsqlUnknownError"),
            Self::MustUseFile => Cow::Borrowed("LibsqlMustUseFileError"),
            Self::UsedReservedTablePrefix => Cow::Borrowed("LibsqlReservedTableError"),
            Self::SchemaNameNotAllowed => Cow::Borrowed("LibsqlSchemaNotAllowedError"),
            Self::IncorrectSqlType(_, _) => Cow::Borrowed("LibsqlIncorrectSqlTypeError"),
            Self::BackupInProgress => Cow::Borrowed("LibsqlBackupInProgressError"),
            Self::Libsql(_) => Cow::Borrowed("LibsqlError"),
            Self::Io(_) => Cow::Borrowed("LibsqlIoError"),
        }
    }

    fn get_message(&self) -> Cow<'static, str> {
        Cow::Owned(self.to_string())
    }

    fn get_additional_properties(
        &self,
    ) -> Box<dyn Iterator<Item = (Cow<'static, str>, PropertyValue)>> {
        Box::new(std::iter::empty())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<libsql::Error> for LibsqlError {
    fn from(err: libsql::Error) -> Self {
        Self::Libsql(err.to_string())
    }
}

impl From<std::io::Error> for LibsqlError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}
