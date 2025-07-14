//! Unified stream storage type for different storage backends
//!
//! This module provides a unified storage type that can wrap different
//! storage backends for stream data, similar to GlobalStorageType and UnifiedGroupStorage.

use crate::storage_backends::{
    StorageIterator,
    traits::{
        MaintenanceResult, SnapshotMetadata, SnapshotStorage, StorageCapabilities, StorageEngine,
        StorageHints, StorageMetrics as StorageMetricsTrait, StorageStats,
    },
    types::{StorageKey, StorageNamespace, StorageResult, StorageValue, WriteBatch},
    {memory::MemoryStorage, rocksdb::RocksDBStorage},
};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::RangeBounds;
use std::sync::Arc;

/// Iterator wrapper for unified stream storage
pub enum UnifiedStreamIterator {
    /// Memory storage iterator
    Memory(crate::storage_backends::memory::MemoryIterator),
    /// RocksDB storage iterator (also returns MemoryIterator after collecting)
    RocksDB(crate::storage_backends::memory::MemoryIterator),
}

impl StorageIterator for UnifiedStreamIterator {
    fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::RocksDB(iter) => iter.next(),
        }
    }

    fn seek(&mut self, key: &StorageKey) -> StorageResult<()> {
        match self {
            Self::Memory(iter) => iter.seek(key),
            Self::RocksDB(iter) => iter.seek(key),
        }
    }

    fn valid(&self) -> bool {
        match self {
            Self::Memory(iter) => iter.valid(),
            Self::RocksDB(iter) => iter.valid(),
        }
    }
}

/// Unified stream storage type that can be either memory or RocksDB
#[derive(Clone)]
pub enum UnifiedStreamStorage {
    /// Memory-backed stream storage
    Memory(Arc<MemoryStorage>),
    /// RocksDB-backed stream storage
    RocksDB(Arc<RocksDBStorage>),
}

impl UnifiedStreamStorage {
    /// Create a new memory-backed storage
    pub fn memory() -> Self {
        Self::Memory(Arc::new(MemoryStorage::new()))
    }

    /// Create a new RocksDB-backed storage
    pub async fn rocksdb(
        config: crate::storage_backends::rocksdb::RocksDBAdaptorConfig,
    ) -> StorageResult<Self> {
        let storage = RocksDBStorage::new(config).await?;
        Ok(Self::RocksDB(Arc::new(storage)))
    }

    /// Get the storage type name
    pub fn storage_type(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::RocksDB(_) => "rocksdb",
        }
    }
}

#[async_trait]
impl StorageEngine for UnifiedStreamStorage {
    type Iterator = UnifiedStreamIterator;

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
            Self::Memory(storage) => Ok(UnifiedStreamIterator::Memory(
                storage.iter(namespace).await?,
            )),
            Self::RocksDB(storage) => Ok(UnifiedStreamIterator::RocksDB(
                storage.iter(namespace).await?,
            )),
        }
    }

    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator> {
        match self {
            Self::Memory(storage) => Ok(UnifiedStreamIterator::Memory(
                storage.iter_range(namespace, range).await?,
            )),
            Self::RocksDB(storage) => Ok(UnifiedStreamIterator::RocksDB(
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

    // New trait methods
    async fn get_batch(
        &self,
        namespace: &StorageNamespace,
        keys: &[StorageKey],
    ) -> StorageResult<Vec<Option<StorageValue>>> {
        match self {
            Self::Memory(storage) => storage.get_batch(namespace, keys).await,
            Self::RocksDB(storage) => storage.get_batch(namespace, keys).await,
        }
    }

    async fn put_with_hints(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
        hints: StorageHints,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.put_with_hints(namespace, key, value, hints).await,
            Self::RocksDB(storage) => storage.put_with_hints(namespace, key, value, hints).await,
        }
    }

    async fn initialize(&self) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.initialize().await,
            Self::RocksDB(storage) => storage.initialize().await,
        }
    }

    async fn shutdown(&self) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.shutdown().await,
            Self::RocksDB(storage) => storage.shutdown().await,
        }
    }

    async fn maintenance(&self) -> StorageResult<MaintenanceResult> {
        match self {
            Self::Memory(storage) => storage.maintenance().await,
            Self::RocksDB(storage) => storage.maintenance().await,
        }
    }

    fn capabilities(&self) -> StorageCapabilities {
        match self {
            Self::Memory(storage) => storage.capabilities(),
            Self::RocksDB(storage) => storage.capabilities(),
        }
    }

    async fn put_if_absent(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<bool> {
        match self {
            Self::Memory(storage) => storage.put_if_absent(namespace, key, value).await,
            Self::RocksDB(storage) => storage.put_if_absent(namespace, key, value).await,
        }
    }

    async fn compare_and_swap(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
        expected: Option<&StorageValue>,
        new_value: StorageValue,
    ) -> StorageResult<bool> {
        match self {
            Self::Memory(storage) => {
                storage
                    .compare_and_swap(namespace, key, expected, new_value)
                    .await
            }
            Self::RocksDB(storage) => {
                storage
                    .compare_and_swap(namespace, key, expected, new_value)
                    .await
            }
        }
    }
}

// Implement StorageMetrics trait
#[async_trait]
impl StorageMetricsTrait for UnifiedStreamStorage {
    async fn get_stats(&self) -> StorageStats {
        match self {
            Self::Memory(storage) => storage.get_stats().await,
            Self::RocksDB(storage) => storage.get_stats().await,
        }
    }

    async fn reset_stats(&self) {
        match self {
            Self::Memory(storage) => storage.reset_stats().await,
            Self::RocksDB(storage) => storage.reset_stats().await,
        }
    }
}

// Note: LogStorage is implemented for the concrete storage types (MemoryStorage, RocksDBStorage)
// but not for UnifiedStreamStorage to avoid trait ambiguity issues.
// If you need LogStorage operations, use the helper methods below.

impl UnifiedStreamStorage {
    /// Helper method to append a log entry with StreamMetadata
    pub async fn append_stream_entry(
        &self,
        namespace: &StorageNamespace,
        entry: crate::storage_backends::log::LogEntry<
            crate::core::state_machine::LocalStreamMetadata,
        >,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.append_entry(namespace, entry).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.append_entry(namespace, entry).await
            }
        }
    }

    /// Helper method to get entry with StreamMetadata
    pub async fn get_stream_entry(
        &self,
        namespace: &StorageNamespace,
        index: u64,
    ) -> StorageResult<
        Option<
            crate::storage_backends::log::LogEntry<crate::core::state_machine::LocalStreamMetadata>,
        >,
    > {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_entry(namespace, index).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_entry(namespace, index).await
            }
        }
    }

    /// Helper method to append multiple entries with StreamMetadata
    pub async fn append_stream_entries(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<
            crate::storage_backends::log::LogEntry<crate::core::state_machine::LocalStreamMetadata>,
        >,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.append_entries(namespace, entries).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.append_entries(namespace, entries).await
            }
        }
    }

    /// Helper method to get last entry with StreamMetadata
    pub async fn get_last_stream_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<
        Option<
            crate::storage_backends::log::LogEntry<crate::core::state_machine::LocalStreamMetadata>,
        >,
    > {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_last_entry(namespace).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_last_entry(namespace).await
            }
        }
    }

    /// Helper method to get first entry with StreamMetadata
    pub async fn get_first_stream_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<
        Option<
            crate::storage_backends::log::LogEntry<crate::core::state_machine::LocalStreamMetadata>,
        >,
    > {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_first_entry(namespace).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.get_first_entry(namespace).await
            }
        }
    }

    /// Helper method to read range with StreamMetadata
    pub async fn read_stream_range<R: RangeBounds<u64> + Send>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> StorageResult<
        Vec<
            crate::storage_backends::log::LogEntry<crate::core::state_machine::LocalStreamMetadata>,
        >,
    > {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.read_range(namespace, range).await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                storage.read_range(namespace, range).await
            }
        }
    }

    /// Helper method to get log state
    pub async fn get_stream_log_state(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<crate::storage_backends::log::LogState>> {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                <_ as LogStorage<crate::core::state_machine::LocalStreamMetadata>>::get_log_state(
                    storage.as_ref(),
                    namespace,
                )
                .await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                <_ as LogStorage<crate::core::state_machine::LocalStreamMetadata>>::get_log_state(
                    storage.as_ref(),
                    namespace,
                )
                .await
            }
        }
    }

    /// Helper method to truncate log
    pub async fn truncate_stream_log(
        &self,
        namespace: &StorageNamespace,
        after_index: u64,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => {
                use crate::storage_backends::log::LogStorage;
                <_ as LogStorage<crate::core::state_machine::LocalStreamMetadata>>::truncate(
                    storage.as_ref(),
                    namespace,
                    after_index,
                )
                .await
            }
            Self::RocksDB(storage) => {
                use crate::storage_backends::log::LogStorage;
                <_ as LogStorage<crate::core::state_machine::LocalStreamMetadata>>::truncate(
                    storage.as_ref(),
                    namespace,
                    after_index,
                )
                .await
            }
        }
    }
}

// Implement SnapshotStorage if needed
#[async_trait]
impl SnapshotStorage for UnifiedStreamStorage {
    async fn create_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<Bytes> {
        match self {
            Self::Memory(storage) => storage.create_snapshot(namespace, snapshot_id).await,
            Self::RocksDB(storage) => storage.create_snapshot(namespace, snapshot_id).await,
        }
    }

    async fn restore_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
        data: Bytes,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.restore_snapshot(namespace, snapshot_id, data).await,
            Self::RocksDB(storage) => storage.restore_snapshot(namespace, snapshot_id, data).await,
        }
    }

    async fn list_snapshots(&self, namespace: &StorageNamespace) -> StorageResult<Vec<String>> {
        match self {
            Self::Memory(storage) => storage.list_snapshots(namespace).await,
            Self::RocksDB(storage) => storage.list_snapshots(namespace).await,
        }
    }

    async fn delete_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<()> {
        match self {
            Self::Memory(storage) => storage.delete_snapshot(namespace, snapshot_id).await,
            Self::RocksDB(storage) => storage.delete_snapshot(namespace, snapshot_id).await,
        }
    }

    async fn snapshot_metadata(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<SnapshotMetadata> {
        match self {
            Self::Memory(storage) => storage.snapshot_metadata(namespace, snapshot_id).await,
            Self::RocksDB(storage) => storage.snapshot_metadata(namespace, snapshot_id).await,
        }
    }
}
