use std::{num::TryFromIntError, sync::Arc};

use proven_stream::StreamError;
use thiserror::Error;

/// An error that can occur while working with a Radix stream.
#[derive(Debug, Error)]
pub enum Error<TSE, ESE>
where
    TSE: StreamError,
    ESE: StreamError,
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

    /// An error occurred in the event stream.
    #[error(transparent)]
    EventStream(ESE),

    /// An error occurred in the transaction stream.
    #[error(transparent)]
    TransactionStream(TSE),

    /// Bad int conversion.
    #[error("Bad int conversion: {0}, {1}")]
    TryFromInt(&'static str, TryFromIntError),
}

impl<TSE, ESE> From<ciborium::de::Error<std::io::Error>> for Error<TSE, ESE>
where
    TSE: StreamError,
    ESE: StreamError,
{
    fn from(error: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborDeserialize(Arc::new(error))
    }
}

impl<ESE, TSE> From<ciborium::ser::Error<std::io::Error>> for Error<TSE, ESE>
where
    ESE: StreamError,
    TSE: StreamError,
{
    fn from(error: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborSerialize(Arc::new(error))
    }
}
