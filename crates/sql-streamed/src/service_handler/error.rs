use thiserror::Error;

/// Errors that can occur in a SQL stream handler.
#[derive(Debug, Error)]
pub enum Error {
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
    #[error("snapshot store error: {0}")]
    SnapshotStore(String),

    /// An error occurred in the stream.
    #[error("stream error: {0}")]
    Stream(String),

    /// An error occurred while creating a temporary file.
    #[error(transparent)]
    TempFile(std::io::Error),
}
