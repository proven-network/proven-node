#![allow(clippy::type_complexity)]

use std::fmt::Debug;

use deno_error::JsError;
use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error, JsError)]
pub enum Error {
    /// The caught up channel was closed unexpectedly.
    #[class(generic)]
    #[error("caught up channel closed")]
    CaughtUpChannelClosed,

    /// An error occurred in the client.
    #[class(generic)]
    #[error("client error: {0}")]
    Client(String),

    /// An error occurred while decoding a leader name.
    #[class(generic)]
    #[error("invalid UTF-8 in leader name")]
    InvalidLeaderName(#[from] std::string::FromUtf8Error),

    /// An error occurred in libsql.
    #[class(generic)]
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// An error occurred in the client.
    #[class(generic)]
    #[error("service error: {0}")]
    Service(String),

    /// An error occurred in the stream.
    #[class(generic)]
    #[error("stream error: {0}")]
    Stream(String),
}

impl SqlStoreError for Error {}
