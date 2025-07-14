//! Core traits for the storage adaptor layer

use crate::storage_backends::types::{
    StorageError, StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue,
    WriteBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::ops::RangeBounds;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Storage hints for optimization
#[derive(Clone, Debug, Default)]
pub struct StorageHints {
    /// Hint that this write is part of a larger batch
    pub is_batch_write: bool,
    /// Hint about expected access pattern
    pub access_pattern: AccessPattern,
    /// Whether this operation can be eventually consistent
    pub allow_eventual_consistency: bool,
    /// Priority level for this operation
    pub priority: Priority,
}

/// Access pattern hints
#[derive(Clone, Debug, Default, PartialEq)]
pub enum AccessPattern {
    /// Sequential access pattern
    Sequential,
    /// Random access pattern
    #[default]
    Random,
    /// Write once, read many times
    WriteOnce,
    /// Temporary data that will be deleted soon
    Temporary,
}

/// Operation priority
#[derive(Clone, Debug, Default, PartialEq, Ord, PartialOrd, Eq)]
pub enum Priority {
    /// Low priority
    Low,
    /// Normal priority
    #[default]
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

/// Storage capabilities advertised by backends
#[derive(Clone, Debug)]
pub struct StorageCapabilities {
    /// Supports atomic batch operations
    pub atomic_batches: bool,
    /// Supports efficient range scans
    pub efficient_range_scan: bool,
    /// Supports snapshots
    pub snapshots: bool,
    /// Is eventually consistent
    pub eventual_consistency: bool,
    /// Maximum value size (None = unlimited)
    pub max_value_size: Option<usize>,
    /// Supports conditional operations atomically
    pub atomic_conditionals: bool,
    /// Supports streaming operations
    pub streaming: bool,
    /// Has built-in caching
    pub caching: bool,
}

impl Default for StorageCapabilities {
    fn default() -> Self {
        Self {
            atomic_batches: true,
            efficient_range_scan: true,
            snapshots: false,
            eventual_consistency: false,
            max_value_size: None,
            atomic_conditionals: false,
            streaming: false,
            caching: false,
        }
    }
}

/// Storage operation statistics
#[derive(Clone, Debug, Default)]
pub struct StorageStats {
    /// Total read operations
    pub reads: u64,
    /// Total write operations
    pub writes: u64,
    /// Total delete operations
    pub deletes: u64,
    /// Cache hits (if applicable)
    pub cache_hits: Option<u64>,
    /// Cache misses (if applicable)
    pub cache_misses: Option<u64>,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Average read latency in milliseconds
    pub avg_read_latency_ms: Option<f64>,
    /// Average write latency in milliseconds
    pub avg_write_latency_ms: Option<f64>,
    /// Total errors
    pub errors: u64,
}

/// Maintenance operation result
#[derive(Clone, Debug, Default)]
pub struct MaintenanceResult {
    /// Bytes reclaimed by maintenance
    pub bytes_reclaimed: u64,
    /// Number of entries compacted
    pub entries_compacted: u64,
    /// Duration of maintenance in milliseconds
    pub duration_ms: u64,
    /// Additional details
    pub details: String,
}

/// Main trait for storage engines providing key-value operations
#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Type of iterator returned by this storage engine
    type Iterator: StorageIterator;

    /// Get storage capabilities
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::default()
    }

    /// Initialize storage (called once before use)
    async fn initialize(&self) -> StorageResult<()> {
        Ok(())
    }

    /// Shutdown storage gracefully
    async fn shutdown(&self) -> StorageResult<()> {
        self.flush().await
    }

    /// Perform maintenance tasks
    async fn maintenance(&self) -> StorageResult<MaintenanceResult> {
        Ok(MaintenanceResult::default())
    }

    /// Get a value by key from a namespace
    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>>;

    /// Get multiple values in a single operation
    async fn get_batch(
        &self,
        namespace: &StorageNamespace,
        keys: &[StorageKey],
    ) -> StorageResult<Vec<Option<StorageValue>>> {
        // Default implementation using individual gets
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(namespace, key).await?);
        }
        Ok(results)
    }

    /// Put a key-value pair into a namespace
    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()>;

    /// Put with hints for optimization
    async fn put_with_hints(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
        _hints: StorageHints,
    ) -> StorageResult<()> {
        // Default implementation ignores hints
        self.put(namespace, key, value).await
    }

    /// Put only if key doesn't exist
    async fn put_if_absent(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<bool> {
        // Default implementation (not atomic)
        if self.exists(namespace, &key).await? {
            Ok(false)
        } else {
            self.put(namespace, key, value).await?;
            Ok(true)
        }
    }

    /// Compare and swap
    async fn compare_and_swap(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
        expected: Option<&StorageValue>,
        new_value: StorageValue,
    ) -> StorageResult<bool> {
        // Default implementation (not atomic)
        let current = self.get(namespace, key).await?;
        match (current.as_ref(), expected) {
            (Some(c), Some(e)) if c.as_bytes() == e.as_bytes() => {
                self.put(namespace, key.clone(), new_value).await?;
                Ok(true)
            }
            (None, None) => {
                self.put(namespace, key.clone(), new_value).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

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

    /// Stream a large value
    async fn get_stream(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<Box<dyn AsyncRead + Send + Unpin>>> {
        // Default: convert regular get to stream
        if let Some(value) = self.get(namespace, key).await? {
            Ok(Some(Box::new(std::io::Cursor::new(value.0))))
        } else {
            Ok(None)
        }
    }

    /// Put from a stream
    async fn put_stream(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        mut stream: Box<dyn AsyncRead + Send + Unpin>,
        size_hint: Option<usize>,
    ) -> StorageResult<()> {
        // Default: buffer entire stream
        let mut buffer = Vec::with_capacity(size_hint.unwrap_or(0));
        stream
            .read_to_end(&mut buffer)
            .await
            .map_err(StorageError::Io)?;
        self.put(namespace, key, StorageValue::new(buffer)).await
    }
}

/// Async iterator trait for storage backends
#[async_trait]
pub trait AsyncStorageIterator: Send {
    /// Get next key-value pair
    async fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>>;

    /// Seek to a specific key
    async fn seek(&mut self, key: &StorageKey) -> StorageResult<()>;

    /// Check if iterator is valid
    fn valid(&self) -> bool;

    /// Prefetch next batch of results (optimization hint)
    async fn prefetch(&mut self, _count: usize) -> StorageResult<()> {
        Ok(()) // Default no-op
    }
}

/// Storage metrics trait
#[async_trait]
pub trait StorageMetrics {
    /// Get operation statistics
    async fn get_stats(&self) -> StorageStats;

    /// Reset statistics
    async fn reset_stats(&self);
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

    /// Append multiple log entries atomically
    async fn append_log_batch(
        &self,
        namespace: &StorageNamespace,
        entries: &[(u64, Bytes)],
    ) -> StorageResult<()> {
        // Default: use write batch
        let mut batch = self.create_write_batch();
        for (index, data) in entries {
            batch.put(
                namespace.clone(),
                StorageKey::from_u64(*index),
                StorageValue::new(data.clone()),
            );
        }
        self.write_batch(batch).await
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

    /// Get the last N log entries efficiently
    async fn last_log_entries(
        &self,
        namespace: &StorageNamespace,
        count: usize,
    ) -> StorageResult<Vec<(u64, Bytes)>> {
        // Default: scan all and take last N (inefficient)
        // Backends should override with more efficient implementation
        let all = self.read_log_range(namespace, 0, u64::MAX).await?;
        let len = all.len();
        if len <= count {
            Ok(all)
        } else {
            Ok(all.into_iter().skip(len - count).collect())
        }
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

/// Extension trait for error context
pub trait StorageErrorContext {
    /// Add context to the error
    fn with_context(self, context: &str) -> Self;
}

impl StorageErrorContext for StorageError {
    fn with_context(self, context: &str) -> Self {
        match self {
            StorageError::Backend(msg) => StorageError::Backend(format!("{context}: {msg}")),
            other => StorageError::Backend(format!("{context}: {other:?}")),
        }
    }
}
