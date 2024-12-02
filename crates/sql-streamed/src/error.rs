use std::sync::Arc;

use proven_sql::SqlStoreError;
use proven_store::Store;
use proven_stream::Stream;
use thiserror::Error;

use crate::stream_handler::SqlStreamHandler;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    /// The caught up channel was closed unexpectedly.
    #[error("Caught up channel closed")]
    CaughtUpChannelClosed,

    /// An error occurred while deserializing CBOR.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// An error occurred while serializing CBOR.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// An error occurred while decoding a leader name.
    #[error("Invalid UTF-8 in leader name")]
    InvalidLeaderName(#[from] std::string::FromUtf8Error),

    /// An error occurred in the leader store.
    #[error(transparent)]
    LeaderStore(LS::Error),

    /// An error occurred in libsql.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// An error occurred in the stream.
    #[error(transparent)]
    Stream(S::Error),
}

impl<S, LS> From<ciborium::de::Error<std::io::Error>> for Error<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<S, LS> From<ciborium::ser::Error<std::io::Error>> for Error<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}

impl<S, LS> SqlStoreError for Error<S, LS>
where
    S: Stream<SqlStreamHandler>,
    LS: Store,
{
}
