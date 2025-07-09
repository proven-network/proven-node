//! Generic storage wrapper for different backend types
//!
//! This module provides a unified interface for different storage backends
//! allowing runtime polymorphism while maintaining type safety.

use crate::storage::{
    StorageEngine, StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue,
    WriteBatch,
    log::{CompactionResult, LogBatch, LogEntry, LogState, LogStorage},
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

/// Generic storage that can hold different backend implementations
#[derive(Clone)]
pub enum GenericStorage {
    /// Memory-based storage
    Memory(std::sync::Arc<super::adaptors::memory::MemoryStorage>),
    /// RocksDB-based storage
    RocksDB(std::sync::Arc<super::adaptors::rocksdb::RocksDBStorage>),
}

/// Generic iterator that wraps different iterator types
pub enum GenericIterator {
    /// Memory storage iterator (also used by RocksDB after collecting results)
    Memory(super::adaptors::memory::MemoryIterator),
}

impl StorageIterator for GenericIterator {
    fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>> {
        match self {
            Self::Memory(iter) => iter.next(),
        }
    }

    fn seek(&mut self, key: &StorageKey) -> StorageResult<()> {
        match self {
            Self::Memory(iter) => iter.seek(key),
        }
    }

    fn valid(&self) -> bool {
        match self {
            Self::Memory(iter) => iter.valid(),
        }
    }
}

#[async_trait]
impl StorageEngine for GenericStorage {
    type Iterator = GenericIterator;

    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>> {
        match self {
            Self::Memory(storage) => storage.get(namespace, key).await,
            Self::RocksDB(storage) => storage.get(namespace, key).await,
        }
    }

    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.put(namespace, key, value).await,
            Self::RocksDB(storage) => storage.put(namespace, key, value).await,
        }
    }

    async fn delete(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.delete(namespace, key).await,
            Self::RocksDB(storage) => storage.delete(namespace, key).await,
        }
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.write_batch(batch).await,
            Self::RocksDB(storage) => storage.write_batch(batch).await,
        }
    }

    async fn iter(&self, namespace: &StorageNamespace) -> StorageResult<Self::Iterator> {
        match self {
            Self::Memory(storage) => Ok(GenericIterator::Memory(storage.iter(namespace).await?)),
            Self::RocksDB(storage) => Ok(GenericIterator::Memory(storage.iter(namespace).await?)),
        }
    }

    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator> {
        match self {
            Self::Memory(storage) => Ok(GenericIterator::Memory(
                storage.iter_range(namespace, range).await?,
            )),
            Self::RocksDB(storage) => Ok(GenericIterator::Memory(
                storage.iter_range(namespace, range).await?,
            )),
        }
    }

    async fn create_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.create_namespace(namespace).await,
            Self::RocksDB(storage) => storage.create_namespace(namespace).await,
        }
    }

    async fn drop_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.drop_namespace(namespace).await,
            Self::RocksDB(storage) => storage.drop_namespace(namespace).await,
        }
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<StorageNamespace>> {
        match self {
            Self::Memory(storage) => storage.list_namespaces().await,
            Self::RocksDB(storage) => storage.list_namespaces().await,
        }
    }

    async fn flush(&self) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.flush().await,
            Self::RocksDB(storage) => storage.flush().await,
        }
    }

    async fn namespace_size(&self, namespace: &StorageNamespace) -> StorageResult<u64> {
        match self {
            Self::Memory(storage) => storage.namespace_size(namespace).await,
            Self::RocksDB(storage) => storage.namespace_size(namespace).await,
        }
    }
}

impl GenericStorage {
    /// Create a memory-backed generic storage
    pub fn memory() -> Self {
        Self::Memory(std::sync::Arc::new(
            super::adaptors::memory::MemoryStorage::new(),
        ))
    }

    /// Create a RocksDB-backed generic storage
    pub async fn rocksdb(
        config: super::adaptors::rocksdb::RocksDBAdaptorConfig,
    ) -> StorageResult<Self> {
        let storage = super::adaptors::rocksdb::RocksDBStorage::new(config).await?;
        Ok(Self::RocksDB(std::sync::Arc::new(storage)))
    }

    /// Get the storage type name
    pub fn storage_type(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::RocksDB(_) => "rocksdb",
        }
    }
}

// Implement LogStorage for GenericStorage with any metadata type
#[async_trait]
impl<M> LogStorage<M> for GenericStorage
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de> + Clone + 'static,
{
    async fn append_entry(
        &self,
        namespace: &StorageNamespace,
        entry: LogEntry<M>,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.append_entry(namespace, entry).await,
            Self::RocksDB(storage) => storage.append_entry(namespace, entry).await,
        }
    }

    async fn get_log_state(&self, namespace: &StorageNamespace) -> StorageResult<Option<LogState>> {
        match self {
            Self::Memory(storage) => {
                <super::adaptors::memory::MemoryStorage as LogStorage<M>>::get_log_state(
                    storage.as_ref(),
                    namespace,
                )
                .await
            }
            Self::RocksDB(storage) => {
                <super::adaptors::rocksdb::RocksDBStorage as LogStorage<M>>::get_log_state(
                    storage.as_ref(),
                    namespace,
                )
                .await
            }
        }
    }

    async fn truncate(&self, namespace: &StorageNamespace, after_index: u64) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => {
                <super::adaptors::memory::MemoryStorage as LogStorage<M>>::truncate(
                    storage.as_ref(),
                    namespace,
                    after_index,
                )
                .await
            }
            Self::RocksDB(storage) => {
                <super::adaptors::rocksdb::RocksDBStorage as LogStorage<M>>::truncate(
                    storage.as_ref(),
                    namespace,
                    after_index,
                )
                .await
            }
        }
    }

    async fn purge(&self, namespace: &StorageNamespace, up_to_index: u64) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => {
                <super::adaptors::memory::MemoryStorage as LogStorage<M>>::purge(
                    storage.as_ref(),
                    namespace,
                    up_to_index,
                )
                .await
            }
            Self::RocksDB(storage) => {
                <super::adaptors::rocksdb::RocksDBStorage as LogStorage<M>>::purge(
                    storage.as_ref(),
                    namespace,
                    up_to_index,
                )
                .await
            }
        }
    }

    async fn create_batch(&self) -> Box<dyn LogBatch<M>> {
        match self {
            Self::Memory(storage) => storage.create_batch().await,
            Self::RocksDB(storage) => storage.create_batch().await,
        }
    }

    async fn apply_batch(&self, batch: Box<dyn LogBatch<M>>) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.apply_batch(batch).await,
            Self::RocksDB(storage) => storage.apply_batch(batch).await,
        }
    }

    async fn read_range<R: RangeBounds<u64> + Send>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> StorageResult<Vec<LogEntry<M>>> {
        match self {
            Self::Memory(storage) => storage.read_range(namespace, range).await,
            Self::RocksDB(storage) => storage.read_range(namespace, range).await,
        }
    }

    async fn get_entry(
        &self,
        namespace: &StorageNamespace,
        index: u64,
    ) -> StorageResult<Option<LogEntry<M>>> {
        match self {
            Self::Memory(storage) => storage.get_entry(namespace, index).await,
            Self::RocksDB(storage) => storage.get_entry(namespace, index).await,
        }
    }

    async fn get_last_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry<M>>> {
        match self {
            Self::Memory(storage) => storage.get_last_entry(namespace).await,
            Self::RocksDB(storage) => storage.get_last_entry(namespace).await,
        }
    }

    async fn get_first_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry<M>>> {
        match self {
            Self::Memory(storage) => storage.get_first_entry(namespace).await,
            Self::RocksDB(storage) => storage.get_first_entry(namespace).await,
        }
    }

    async fn append_entries(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<LogEntry<M>>,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.append_entries(namespace, entries).await,
            Self::RocksDB(storage) => storage.append_entries(namespace, entries).await,
        }
    }

    async fn read_time_range(
        &self,
        namespace: &StorageNamespace,
        start_time: u64,
        end_time: u64,
    ) -> StorageResult<Vec<LogEntry<M>>> {
        match self {
            Self::Memory(storage) => {
                storage
                    .read_time_range(namespace, start_time, end_time)
                    .await
            }
            Self::RocksDB(storage) => {
                storage
                    .read_time_range(namespace, start_time, end_time)
                    .await
            }
        }
    }

    async fn compact(
        &self,
        namespace: &StorageNamespace,
        up_to_index: u64,
    ) -> StorageResult<CompactionResult> {
        match self {
            Self::Memory(storage) => {
                <super::adaptors::memory::MemoryStorage as LogStorage<M>>::compact(
                    storage.as_ref(),
                    namespace,
                    up_to_index,
                )
                .await
            }
            Self::RocksDB(storage) => {
                <super::adaptors::rocksdb::RocksDBStorage as LogStorage<M>>::compact(
                    storage.as_ref(),
                    namespace,
                    up_to_index,
                )
                .await
            }
        }
    }
}
