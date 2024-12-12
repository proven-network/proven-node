#![allow(clippy::type_complexity)]

use crate::Request;
use crate::SqlStreamHandler;

use std::error::Error as StdError;
use std::fmt::Debug;

use proven_messaging::client::Client;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::stream::{InitializedStream, Stream, StreamError};
use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<
    P,
    X = SqlStreamHandler,
    T = Request,
    D = ciborium::de::Error<std::io::Error>,
    S = ciborium::ser::Error<std::io::Error>,
> where
    P: Stream<T, D, S>,
    X: ServiceHandler<T, D, S>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<bytes::Bytes, Error = D>
        + TryInto<bytes::Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
    P::Initialized: InitializedStream<T, D, S>,
    <P::Initialized as InitializedStream<T, D, S>>::Error: StreamError,
{
    /// The caught up channel was closed unexpectedly.
    #[error("Caught up channel closed")]
    CaughtUpChannelClosed,

    /// An error occurred in the client.
    #[error("client error")]
    Client(
        <<P::Initialized as InitializedStream<T, D, S>>::Client<X> as Client<X, T, D, S>>::Error,
    ),

    /// An error occurred while decoding a leader name.
    #[error("Invalid UTF-8 in leader name")]
    InvalidLeaderName(#[from] std::string::FromUtf8Error),

    /// An error occurred in libsql.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// An error occurred in the stream.
    #[error(transparent)]
    Stream(<P::Initialized as InitializedStream<T, D, S>>::Error),
}

impl<P> SqlStoreError for Error<P> where
    P: Stream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>
{
}
