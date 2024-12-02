use std::sync::Arc;

use proven_stream::StreamHandlerError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}

impl StreamHandlerError for Error {}
