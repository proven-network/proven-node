use deno_error::JsError;
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error, JsError)]
pub enum Error {
    /// Failed to create a key-value store.
    #[class(generic)]
    #[error("failed to create key-value store: {0}")]
    CreateKeyValue(async_nats::jetstream::context::CreateKeyValueErrorKind),

    /// Failed to delete from key-value store.
    #[class(generic)]
    #[error("failed to delete from key-value store: {0}")]
    Delete(async_nats::jetstream::kv::DeleteErrorKind),

    /// Deserialization error.
    #[class(generic)]
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// Failed to get from key-value store.
    #[class(generic)]
    #[error("failed to get from key-value store: {0}")]
    Entry(async_nats::jetstream::kv::EntryErrorKind),

    /// Failed to put to key-value store.
    #[class(generic)]
    #[error("failed to put to key-value store: {0}")]
    Put(async_nats::jetstream::kv::PutErrorKind),

    /// Serialization error.
    #[class(generic)]
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Failed to list keys from key-value store.
    #[class(generic)]
    #[error("failed to list keys from key-value store: {0}")]
    Watch(async_nats::jetstream::kv::WatchErrorKind),
}

impl StoreError for Error {}
