use proven_locks::LockManagerError;
use thiserror::Error;

/// Errors that can occur when using the NATS lock manager.
#[derive(Error, Debug)]
pub enum Error {
    /// Error creating the KV store.
    #[error("NATS JetStream context error creating KV store: {0}")]
    CreateKvError(#[from] async_nats::jetstream::context::CreateKeyValueError),

    /// Error creating a lock.
    #[error("NATS KV store create operation error: {0}")]
    CreateError(#[from] async_nats::jetstream::kv::CreateError),

    /// Error getting a lock.
    #[error("NATS KV store entry operation error (get): {0}")]
    EntryError(#[from] async_nats::jetstream::kv::EntryError),
}

impl LockManagerError for Error {}
