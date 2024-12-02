use std::sync::Arc;

use proven_sql::SqlStoreError;
use proven_store::StoreError;
use proven_stream::StreamError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<SE, LSE>
where
    SE: StreamError,
    LSE: StoreError,
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
    LeaderStore(LSE),

    /// An error occurred in libsql.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// An error occurred in the stream.
    #[error(transparent)]
    Stream(SE),
}

impl<SE, LSE> From<ciborium::de::Error<std::io::Error>> for Error<SE, LSE>
where
    SE: StreamError,
    LSE: StoreError,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<SE, LSE> From<ciborium::ser::Error<std::io::Error>> for Error<SE, LSE>
where
    SE: StreamError,
    LSE: StoreError,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}

impl<SE, LSE> SqlStoreError for Error<SE, LSE>
where
    SE: StreamError,
    LSE: StoreError,
{
}
