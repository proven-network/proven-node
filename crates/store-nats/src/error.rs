use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("failed to create key-value store: {0}")]
    CreateKeyValue(async_nats::jetstream::context::CreateKeyValueErrorKind),

    #[error("failed to delete from key-value store: {0}")]
    Delete(async_nats::jetstream::kv::DeleteErrorKind),

    #[error("failed to get from key-value store: {0}")]
    Entry(async_nats::jetstream::kv::EntryErrorKind),

    #[error("failed to put to key-value store: {0}")]
    Put(async_nats::jetstream::kv::PutErrorKind),

    #[error("failed to list keys from key-value store: {0}")]
    Watch(async_nats::jetstream::kv::WatchErrorKind),
}
