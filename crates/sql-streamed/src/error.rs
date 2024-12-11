use std::sync::Arc;

use proven_messaging::stream::StreamError;
use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<SE>
where
    SE: StreamError,
{
    /// The caught up channel was closed unexpectedly.
    #[error("Caught up channel closed")]
    CaughtUpChannelClosed,

    /// An error occurred in the client.
    #[error("client error")]
    Client,

    /// An error occurred while deserializing CBOR.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// An error occurred while serializing CBOR.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// An error occurred while decoding a leader name.
    #[error("Invalid UTF-8 in leader name")]
    InvalidLeaderName(#[from] std::string::FromUtf8Error),

    /// An error occurred in libsql.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// An error occurred in the stream.
    #[error(transparent)]
    Stream(SE),
}

impl<SE> SqlStoreError for Error<SE> where SE: StreamError {}
