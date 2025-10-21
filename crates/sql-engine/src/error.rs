//! Error types for the SQL engine.

use deno_error::{JsErrorClass, PropertyValue};
use proven_sql::SqlStoreError;
use std::borrow::Cow;
use thiserror::Error;

/// SQL engine errors.
#[derive(Error, Debug)]
pub enum Error {
    /// Client error
    #[error("Client error: {0}")]
    Client(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Engine/Stream error
    #[error("Stream error: {0}")]
    Stream(String),

    /// Leadership error
    #[error("Leadership error: {0}")]
    Leadership(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Service error
    #[error("Service error: {0}")]
    Service(String),

    /// Backup is in progress
    #[error("Backup is in progress")]
    BackupInProgress,

    /// `LibSQL` error
    #[error("LibSQL error: {0}")]
    Libsql(#[from] crate::libsql_error::LibsqlError),

    /// IO error
    #[error("IO error: {0}")]
    Io(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Request timeout
    #[error("Request timed out")]
    RequestTimeout,

    /// Channel closed
    #[error("Channel closed")]
    ChannelClosed,
}

impl JsErrorClass for Error {
    fn get_class(&self) -> Cow<'static, str> {
        match self {
            Self::Client(_) => Cow::Borrowed("SqlClientError"),
            Self::Deserialization(_) => Cow::Borrowed("SqlDeserializationError"),
            Self::Stream(_) => Cow::Borrowed("SqlStreamError"),
            Self::Leadership(_) => Cow::Borrowed("SqlLeadershipError"),
            Self::Timeout(_) => Cow::Borrowed("SqlTimeoutError"),
            Self::Service(_) => Cow::Borrowed("SqlServiceError"),
            Self::BackupInProgress => Cow::Borrowed("SqlBackupInProgressError"),
            Self::Libsql(e) => e.get_class(),
            Self::Io(_) => Cow::Borrowed("SqlIoError"),
            Self::Serialization(_) => Cow::Borrowed("SqlSerializationError"),
            Self::RequestTimeout => Cow::Borrowed("SqlRequestTimeoutError"),
            Self::ChannelClosed => Cow::Borrowed("SqlChannelClosedError"),
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

    fn get_ref(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
}

impl SqlStoreError for Error {}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Self::Deserialization(err.to_string())
    }
}

impl From<proven_engine::error::Error> for Error {
    fn from(err: proven_engine::error::Error) -> Self {
        Self::Stream(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<tempfile::PersistError> for Error {
    fn from(err: tempfile::PersistError) -> Self {
        Self::Io(err.to_string())
    }
}
