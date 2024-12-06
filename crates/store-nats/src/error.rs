use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
    /// Failed to create a key-value store.
    #[error("failed to create key-value store: {0}")]
    CreateKeyValue(async_nats::jetstream::context::CreateKeyValueErrorKind),

    /// Failed to delete from key-value store.
    #[error("failed to delete from key-value store: {0}")]
    Delete(async_nats::jetstream::kv::DeleteErrorKind),

    /// Deserialization error.
    #[error(transparent)]
    Deserialize(DE),

    /// Failed to get from key-value store.
    #[error("failed to get from key-value store: {0}")]
    Entry(async_nats::jetstream::kv::EntryErrorKind),

    /// Failed to put to key-value store.
    #[error("failed to put to key-value store: {0}")]
    Put(async_nats::jetstream::kv::PutErrorKind),

    /// Serialization error.
    #[error(transparent)]
    Serialize(SE),

    /// Failed to list keys from key-value store.
    #[error("failed to list keys from key-value store: {0}")]
    Watch(async_nats::jetstream::kv::WatchErrorKind),
}

impl<DE, SE> StoreError for Error<DE, SE>
where
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
{
}
