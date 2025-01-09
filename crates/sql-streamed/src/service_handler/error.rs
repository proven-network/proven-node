use std::convert::Infallible;

use bytes::Bytes;
use proven_store::Store;
use thiserror::Error;

/// Errors that can occur in a SQL stream handler.
#[derive(Debug, Error)]
pub enum Error<SS>
where
    SS: Store<Bytes, Infallible, Infallible>,
{
    /// The caught-up channel was closed.
    #[error("caught-up channel closed")]
    CaughtUpChannelClosed,

    /// Libsql error.
    #[error(transparent)]
    Libsql(#[from] proven_libsql::Error),

    /// Expected a snapshot but none was found.
    #[error("snapshot not found")]
    SnapshotNotFound,

    /// An error occured in the snapshot store.
    #[error(transparent)]
    SnapshotStore(SS::Error),

    /// An error occurred while creating a temporary file.
    #[error(transparent)]
    TempFile(std::io::Error),
}
