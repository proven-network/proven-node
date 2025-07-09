//! Core traits for the storage adaptor layer

use crate::storage::types::{
    StorageError, StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue,
    WriteBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::RangeBounds;

/// Main trait for storage engines providing key-value operations
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Type of iterator returned by this storage engine
    type Iterator: StorageIterator;

    /// Get a value by key from a namespace
    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>>;

    /// Put a key-value pair into a namespace
    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()>;

    /// Delete a key from a namespace
    async fn delete(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<()>;

    /// Check if a key exists in a namespace
    async fn exists(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<bool> {
        Ok(self.get(namespace, key).await?.is_some())
    }

    /// Execute a batch of operations atomically
    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()>;

    /// Create an iterator over a namespace
    async fn iter(&self, namespace: &StorageNamespace) -> StorageResult<Self::Iterator>;

    /// Create an iterator over a range of keys in a namespace
    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator>;

    /// Create a new namespace (e.g., column family in RocksDB)
    async fn create_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()>;

    /// Drop a namespace and all its data
    async fn drop_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()>;

    /// List all namespaces
    async fn list_namespaces(&self) -> StorageResult<Vec<StorageNamespace>>;

    /// Flush any pending writes to persistent storage
    async fn flush(&self) -> StorageResult<()>;

    /// Get approximate size of a namespace in bytes
    async fn namespace_size(&self, namespace: &StorageNamespace) -> StorageResult<u64>;

    /// Create a write batch for atomic operations
    fn create_write_batch(&self) -> WriteBatch {
        WriteBatch::new()
    }

    /// Scan a range of keys (for compatibility with generic wrapper)
    async fn scan(
        &self,
        namespace: &StorageNamespace,
        start_key: &StorageKey,
        end_key: Option<&StorageKey>,
    ) -> StorageResult<Self::Iterator> {
        // Default implementation uses iter_range
        match end_key {
            Some(end) => {
                self.iter_range(namespace, start_key.clone()..end.clone())
                    .await
            }
            None => self.iter_range(namespace, start_key.clone()..).await,
        }
    }
}

/// Trait for sequential log storage (optimized for append-only workloads)
#[async_trait]
pub trait LogStorage: StorageEngine {
    /// Append an entry to a log
    async fn append_log(
        &self,
        namespace: &StorageNamespace,
        index: u64,
        data: Bytes,
    ) -> StorageResult<()> {
        self.put(
            namespace,
            StorageKey::from_u64(index),
            StorageValue::new(data),
        )
        .await
    }

    /// Read a range of log entries
    async fn read_log_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: u64,
    ) -> StorageResult<Vec<(u64, Bytes)>> {
        let start_key = StorageKey::from_u64(start);
        let end_key = StorageKey::from_u64(end);

        let mut entries = Vec::new();
        let mut iter = self.iter_range(namespace, start_key..=end_key).await?;

        while let Some((key, value)) = iter.next()? {
            // Parse the key back to u64
            if key.as_bytes().len() == 8 {
                let index = u64::from_be_bytes(
                    key.as_bytes()
                        .try_into()
                        .map_err(|_| StorageError::InvalidKey("Invalid log index".to_string()))?,
                );
                entries.push((index, value.0));
            }
        }

        Ok(entries)
    }

    /// Get the last log index in a namespace
    async fn last_log_index(&self, namespace: &StorageNamespace) -> StorageResult<Option<u64>> {
        // This is a default implementation; backends can optimize this
        let mut iter = self.iter(namespace).await?;
        let mut last_index = None;

        while let Some((key, _)) = iter.next()? {
            if key.as_bytes().len() == 8 {
                let index = u64::from_be_bytes(
                    key.as_bytes()
                        .try_into()
                        .map_err(|_| StorageError::InvalidKey("Invalid log index".to_string()))?,
                );
                last_index = Some(index);
            }
        }

        Ok(last_index)
    }

    /// Truncate log entries after a given index
    async fn truncate_log(&self, namespace: &StorageNamespace, after: u64) -> StorageResult<()> {
        let start_key = StorageKey::from_u64(after + 1);
        let mut batch = WriteBatch::new();

        let mut iter = self.iter_range(namespace, start_key.clone()..).await?;

        while let Some((key, _)) = iter.next()? {
            batch.delete(namespace.clone(), key);
        }

        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }

        Ok(())
    }

    /// Purge log entries before a given index
    async fn purge_log(&self, namespace: &StorageNamespace, before: u64) -> StorageResult<()> {
        let end_key = StorageKey::from_u64(before);
        let mut batch = WriteBatch::new();

        let mut iter = self.iter_range(namespace, ..end_key).await?;

        while let Some((key, _)) = iter.next()? {
            batch.delete(namespace.clone(), key);
        }

        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }

        Ok(())
    }
}

/// Trait for snapshot storage operations
#[async_trait]
pub trait SnapshotStorage: StorageEngine {
    /// Create a snapshot of a namespace
    async fn create_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<Bytes>;

    /// Restore from a snapshot
    async fn restore_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
        data: Bytes,
    ) -> StorageResult<()>;

    /// List available snapshots
    async fn list_snapshots(&self, namespace: &StorageNamespace) -> StorageResult<Vec<String>>;

    /// Delete a snapshot
    async fn delete_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<()>;

    /// Get snapshot metadata (size, creation time, etc.)
    async fn snapshot_metadata(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<SnapshotMetadata>;
}

/// Metadata about a snapshot
#[derive(Clone, Debug)]
pub struct SnapshotMetadata {
    /// The snapshot ID
    pub id: String,
    /// The size of the snapshot in bytes
    pub size: u64,
    /// The timestamp when the snapshot was created
    pub created_at: u64,
    /// The namespace of the snapshot
    pub namespace: StorageNamespace,
}
