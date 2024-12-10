use proven_messaging::client::ClientError;
use proven_sql::SqlStoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error<CE>
where
    CE: ClientError,
{
    /// The caught up channel was closed unexpectedly.
    #[error("Caught up channel closed")]
    CaughtUpChannelClosed,

    /// An error occurred in the client.
    #[error(transparent)]
    Client(CE),

    /// An error occurred in libsql.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),
}

impl<CE> SqlStoreError for Error<CE> where CE: ClientError {}
