use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Failed to create a key-value store.
    #[error("failed to create key-value store: {0}")]
    CreateKeyValue(async_nats::jetstream::context::CreateKeyValueErrorKind),

    /// Failed to delete from key-value store.
    #[error("failed to delete from key-value store: {0}")]
    Delete(async_nats::jetstream::kv::DeleteErrorKind),

    /// Failed to get from key-value store.
    #[error("failed to get from key-value store: {0}")]
    Entry(async_nats::jetstream::kv::EntryErrorKind),

    /// Failed to put to key-value store.
    #[error("failed to put to key-value store: {0}")]
    Put(async_nats::jetstream::kv::PutErrorKind),

    /// Failed to list keys from key-value store.
    #[error("failed to list keys from key-value store: {0}")]
    Watch(async_nats::jetstream::kv::WatchErrorKind),
}

impl StoreError for Error {}
