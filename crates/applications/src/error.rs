use std::sync::Arc;

use proven_store::StoreError;
use thiserror::Error;

pub type Result<T, SE> = std::result::Result<T, Error<SE>>;

#[derive(Clone, Debug, Error)]
pub enum Error<SE> {
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    #[error(transparent)]
    Store(SE),
}

impl<SE: StoreError> From<ciborium::de::Error<std::io::Error>> for Error<SE> {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Error::CborDeserialize(Arc::new(error))
    }
}

impl<SE: StoreError> From<ciborium::ser::Error<std::io::Error>> for Error<SE> {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Error::CborSerialize(Arc::new(error))
    }
}
