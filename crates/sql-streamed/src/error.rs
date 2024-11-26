use std::sync::Arc;

use proven_sql::SqlStoreError;
use proven_store::StoreError;
use proven_stream::{StreamError, StreamHandlerError};
use thiserror::Error;

pub type Result<T, SE, LSE> = std::result::Result<T, Error<SE, LSE>>;

#[derive(Clone, Debug, Error)]
pub enum Error<SE, LSE> {
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error("Invalid UTF-8 in leader name")]
    InvalidLeaderName(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    LeaderStore(LSE),

    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    #[error(transparent)]
    Stream(SE),
}

impl<SE: StreamError, LSE: StoreError> From<ciborium::de::Error<std::io::Error>>
    for Error<SE, LSE>
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Error::CborDeserialize(Arc::new(error))
    }
}

impl<SE: StreamError, LSE: StoreError> From<ciborium::ser::Error<std::io::Error>>
    for Error<SE, LSE>
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::CborSerialize(Arc::new(error))
    }
}

impl<SE: StreamError, LSE: StoreError> SqlStoreError for Error<SE, LSE> {}

pub type HandlerResult<T> = std::result::Result<T, HandlerError>;

#[derive(Clone, Debug, Error)]
pub enum HandlerError {
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl From<ciborium::de::Error<std::io::Error>> for HandlerError {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        HandlerError::CborDeserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for HandlerError {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        HandlerError::CborSerialize(Arc::new(error))
    }
}

impl StreamHandlerError for HandlerError {}
