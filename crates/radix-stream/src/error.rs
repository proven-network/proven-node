use std::{num::TryFromIntError, sync::Arc};

use thiserror::Error;

/// An error that can occur while working with a Radix stream.
#[derive(Debug, Error)]
pub enum Error {
    /// The stream has already started.
    #[error("the stream has already started")]
    AlreadyStarted,

    /// An error occurred while deserializing CBOR.
    #[error(transparent)]
    Deserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// An error occurred while serializing CBOR.
    #[error(transparent)]
    Serialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// An error occurred in the transaction stream.
    #[error("transaction stream error: {0}")]
    TransactionStream(String),

    /// Bad int conversion.
    #[error("bad int conversion: {0}, {1}")]
    TryFromInt(&'static str, TryFromIntError),
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::Deserialize(Arc::new(error))
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Serialize(Arc::new(error))
    }
}
