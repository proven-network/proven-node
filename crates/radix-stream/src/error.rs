use std::sync::Arc;

use proven_stream::StreamError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error<TSE>
where
    TSE: StreamError,
{
    /// The stream has already started.
    #[error("The stream has already started")]
    AlreadyStarted,

    /// An error occurred while deserializing CBOR.
    #[error(transparent)]
    CborDeserialize(Arc<ciborium::de::Error<std::io::Error>>),

    /// An error occurred while serializing CBOR.
    #[error(transparent)]
    CborSerialize(Arc<ciborium::ser::Error<std::io::Error>>),

    /// An error occurred in the transaction stream.
    #[error(transparent)]
    TransactionStream(TSE),
}

impl<TSE> From<ciborium::de::Error<std::io::Error>> for Error<TSE>
where
    TSE: StreamError,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<TSE> From<ciborium::ser::Error<std::io::Error>> for Error<TSE>
where
    TSE: StreamError,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
