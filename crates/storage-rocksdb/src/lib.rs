//! RocksDB log storage implementation

pub mod config;

use async_trait::async_trait;
use bytes::Bytes;
use proven_storage::{
    LogIndex, LogStorage, StorageAdaptor, StorageError, StorageNamespace, StorageResult,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, WriteBatch,
};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::sync::RwLock;
use tokio_stream::Stream;

/// Type alias for log bounds cache to reduce type complexity
type LogBoundsCache = Arc<RwLock<HashMap<StorageNamespace, (LogIndex, LogIndex)>>>;

/// RocksDB log storage implementation
#[derive(Clone)]
pub struct RocksDbStorage {
    /// The RocksDB instance
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    /// Log bounds cache: namespace -> (first_index, last_index)
    log_bounds: LogBoundsCache,
}

impl RocksDbStorage {
    /// Create a new RocksDB storage instance
    pub async fn new(path: impl AsRef<Path>) -> StorageResult<Self> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| StorageError::Backend("Invalid path".to_string()))?;

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // List existing column families if database exists
        let cf_names = match DBWithThreadMode::<MultiThreaded>::list_cf(&db_opts, path_str) {
            Ok(existing) => {
                // Database exists, use existing column families
                if existing.is_empty() {
                    vec!["default".to_string()]
                } else {
                    existing
                }
            }
            Err(_) => {
                // Database doesn't exist, start with default
                vec!["default".to_string()]
            }
        };

        let cfs: Vec<ColumnFamilyDescriptor> = cf_names
            .into_iter()
            .map(|name| {
                let cf_opts = Options::default();
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, path_str, cfs)
            .map_err(|e| StorageError::Backend(format!("Failed to open RocksDB: {e}")))?;

        Ok(Self {
            db: Arc::new(db),
            log_bounds: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get or create a column family for a namespace
    fn get_or_create_cf(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Arc<BoundColumnFamily<'_>>> {
        let cf_name = namespace.as_str();

        // Try to get existing column family
        if let Some(cf) = self.db.cf_handle(cf_name) {
            return Ok(cf);
        }

        // Create new column family
        let opts = Options::default();
        self.db
            .create_cf(cf_name, &opts)
            .map_err(|e| StorageError::Backend(format!("Failed to create column family: {e}")))?;

        self.db.cf_handle(cf_name).ok_or_else(|| {
            StorageError::Backend("Failed to get column family after creation".to_string())
        })
    }

    /// Get or create a metadata column family for a namespace
    fn get_or_create_metadata_cf(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Arc<BoundColumnFamily<'_>>> {
        let cf_name = format!("{}_meta", namespace.as_str());

        // Try to get existing column family
        if let Some(cf) = self.db.cf_handle(&cf_name) {
            return Ok(cf);
        }

        // Create new column family with optimized settings for metadata
        let mut opts = Options::default();
        // Metadata is small and accessed frequently, optimize for point lookups
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.optimize_for_point_lookup(64); // 64MB block cache

        self.db.create_cf(&cf_name, &opts).map_err(|e| {
            StorageError::Backend(format!("Failed to create metadata column family: {e}"))
        })?;

        self.db.cf_handle(&cf_name).ok_or_else(|| {
            StorageError::Backend("Failed to get metadata column family after creation".to_string())
        })
    }

    /// Encode a log key for RocksDB
    fn encode_key(index: LogIndex) -> Vec<u8> {
        index.get().to_be_bytes().to_vec()
    }

    /// Decode a log key from RocksDB
    fn decode_key(key: &[u8]) -> StorageResult<LogIndex> {
        if key.len() != 8 {
            return Err(StorageError::InvalidKey("Invalid key length".to_string()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        let value = u64::from_be_bytes(bytes);
        LogIndex::new(value)
            .ok_or_else(|| StorageError::InvalidKey("Zero index not allowed".to_string()))
    }
}

#[async_trait]
impl LogStorage for RocksDbStorage {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Arc<Vec<Bytes>>,
    ) -> StorageResult<LogIndex> {
        if entries.is_empty() {
            return Err(StorageError::InvalidValue(
                "Cannot append empty entries".to_string(),
            ));
        }

        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        // Get the next index based on current bounds
        let existing_bounds = bounds.get(namespace).copied();
        let start_index = if let Some((_, last)) = existing_bounds {
            LogIndex::new(last.get() + 1).unwrap()
        } else {
            LogIndex::new(1).unwrap()
        };

        // Add all entries to batch sequentially
        let mut last_index = start_index;
        for (i, data) in entries.iter().enumerate() {
            let index = LogIndex::new(start_index.get() + i as u64).unwrap();
            batch.put_cf(&cf, Self::encode_key(index), data.as_ref());
            last_index = index;
        }

        // Write batch
        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache
        let (first, _) = existing_bounds.unwrap_or((start_index, last_index));
        bounds.insert(namespace.clone(), (first, last_index));

        Ok(last_index)
    }

    async fn put_at(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(LogIndex, Arc<Bytes>)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        // Get current bounds
        let existing_bounds = bounds.get(namespace).copied();
        let mut first_index: Option<LogIndex> = existing_bounds.map(|(first, _)| first);
        let mut last_index: Option<LogIndex> = existing_bounds.map(|(_, last)| last);

        // Add all entries to batch
        for (index, data) in entries {
            batch.put_cf(&cf, Self::encode_key(index), data.as_ref());

            // Update bounds
            match first_index {
                None => first_index = Some(index),
                Some(first) if index < first => first_index = Some(index),
                _ => {}
            }

            match last_index {
                None => last_index = Some(index),
                Some(last) if index > last => last_index = Some(index),
                _ => {}
            }
        }

        // Write batch
        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache if we have entries
        if let (Some(first), Some(last)) = (first_index, last_index) {
            bounds.insert(namespace.clone(), (first, last));
        }

        Ok(())
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(LogIndex, Bytes)>> {
        let cf = self.get_or_create_cf(namespace)?;
        let mut entries = Vec::new();

        let start_key = Self::encode_key(start);
        let end_key = Self::encode_key(end);

        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for result in iter {
            let (key, value) =
                result.map_err(|e| StorageError::Backend(format!("Iterator error: {e}")))?;

            // Check if we've reached the end
            if key.as_ref() >= end_key.as_slice() {
                break;
            }

            let index = Self::decode_key(&key)?;
            entries.push((index, Bytes::copy_from_slice(&value)));
        }

        Ok(entries)
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        // Create next index for the range
        let next_index = LogIndex::new(index.get() + 1);

        if let Some(next) = next_index {
            let start_key = Self::encode_key(next);
            let iter = self.db.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
            );

            for result in iter {
                let (key, _) =
                    result.map_err(|e| StorageError::Backend(format!("Iterator error: {e}")))?;
                batch.delete_cf(&cf, key);
            }
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache
        if let Some((first, last)) = bounds.get_mut(namespace) {
            if index < *last {
                *last = index;
            }
            if *first > *last {
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        // We want to delete everything up to and including index
        let next_index = LogIndex::new(index.get() + 1);
        let end_key = next_index.map(Self::encode_key);

        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for result in iter {
            let (key, _) =
                result.map_err(|e| StorageError::Backend(format!("Iterator error: {e}")))?;

            // If we have an end key and we've reached or passed it, stop
            if let Some(ref end) = end_key
                && key.as_ref() >= end.as_slice()
            {
                break;
            }

            batch.delete_cf(&cf, key);
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache
        if let Some((first, last)) = bounds.get_mut(namespace) {
            if let Some(next) = next_index {
                if index >= *first {
                    *first = next;
                }
                if *first > *last {
                    bounds.remove(namespace);
                }
            } else {
                // If next_index overflowed, we're deleting everything
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(LogIndex, LogIndex)>> {
        // First check cache
        {
            let bounds = self.log_bounds.read().await;
            if let Some(&cached) = bounds.get(namespace) {
                return Ok(Some(cached));
            }
        }

        // If not in cache, compute from database
        let cf = self.get_or_create_cf(namespace)?;

        // Find first entry
        let first = {
            let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            match iter.next() {
                Some(Ok((key, _))) => Some(Self::decode_key(&key)?),
                _ => None,
            }
        };

        // Find last entry
        let last = {
            let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
            match iter.next() {
                Some(Ok((key, _))) => Some(Self::decode_key(&key)?),
                _ => None,
            }
        };

        match (first, last) {
            (Some(f), Some(l)) => {
                // Update cache
                let mut bounds = self.log_bounds.write().await;
                bounds.insert(namespace.clone(), (f, l));
                Ok(Some((f, l)))
            }
            _ => Ok(None),
        }
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        let cf = self.get_or_create_metadata_cf(namespace)?;

        match self.db.get_cf(&cf, key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(Bytes::from(value))),
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Backend(format!(
                "Failed to get metadata: {e}"
            ))),
        }
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        let cf = self.get_or_create_metadata_cf(namespace)?;

        self.db
            .put_cf(&cf, key.as_bytes(), value.as_ref())
            .map_err(|e| StorageError::Backend(format!("Failed to set metadata: {e}")))
    }
}

// Implement LogStorageWithDelete for RocksDbStorage
#[async_trait]
impl proven_storage::LogStorageWithDelete for RocksDbStorage {
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<bool> {
        let cf = self.get_or_create_cf(namespace)?;
        let key = Self::encode_key(index);

        // Note: RocksDB doesn't have a single atomic "delete-and-return-existed" operation.
        // We use get_pinned_cf which is optimized to avoid data copying when we only need
        // to check existence. This is the most efficient approach available.
        match self.db.get_pinned_cf(&cf, &key) {
            Ok(Some(_)) => {
                // Entry exists, delete it
                self.db
                    .delete_cf(&cf, &key)
                    .map_err(|e| StorageError::Backend(format!("Failed to delete entry: {e}")))?;
                Ok(true)
            }
            Ok(None) => {
                // Entry doesn't exist
                Ok(false)
            }
            Err(e) => Err(StorageError::Backend(format!("Failed to check entry: {e}"))),
        }
    }
}

// Implement LogStorageStreaming for RocksDbStorage
#[async_trait]
impl proven_storage::LogStorageStreaming for RocksDbStorage {
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: Option<LogIndex>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(LogIndex, Bytes)>> + Send + Unpin>>
    {
        let _cf = self.get_or_create_cf(namespace)?;
        let start_key = Self::encode_key(start);

        // Clone what we need for the stream
        let db = self.db.clone();
        let namespace_str = namespace.as_str().to_string();

        // Create the stream using async_stream
        let stream = async_stream::stream! {
            // Get column family inside the stream
            let cf = match db.cf_handle(&namespace_str) {
                Some(cf) => cf,
                None => {
                    yield Err(StorageError::NamespaceNotFound(namespace_str));
                    return;
                }
            };

            // Create an iterator starting from our key
            let iter = db.iterator_cf(&cf, IteratorMode::From(&start_key, rocksdb::Direction::Forward));

            for item in iter {
                match item {
                    Ok((key, value)) => {
                        // Decode the key
                        match Self::decode_key(&key) {
                            Ok(index) => {
                                // Check if we've reached the end bound
                                if let Some(end_idx) = end
                                    && index >= end_idx {
                                        break;
                                    }
                                yield Ok((index, Bytes::from(value.to_vec())));
                            }
                            Err(e) => {
                                yield Err(e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(StorageError::Backend(format!("Iterator error: {e}")));
                        break;
                    }
                }
            }
        };

        Ok(Box::new(Box::pin(stream)))
    }
}

impl std::fmt::Debug for RocksDbStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RocksDbStorage")
    }
}

impl Drop for RocksDbStorage {
    fn drop(&mut self) {
        // Check if this is the last reference to the database
        // Arc::strong_count is 1 when this is the last reference being dropped
        if Arc::strong_count(&self.db) == 1 {
            tracing::debug!("RocksDbStorage being dropped - this is the last reference");
            // RocksDB will automatically flush and close when the Arc<DB> is dropped
            // This serves as a safety net to ensure proper cleanup
        }
    }
}

// Implement StorageAdaptor for RocksDbStorage
// Since RocksDbStorage already implements LogStorage + LogStorageWithDelete,
// we just need to implement the trait with any additional methods
#[async_trait]
impl StorageAdaptor for RocksDbStorage {
    async fn shutdown(&self) -> StorageResult<()> {
        tracing::info!(
            "Shutting down RocksDB storage, current Arc strong count: {}",
            Arc::strong_count(&self.db)
        );

        // Flush the database to ensure all data is persisted
        self.db
            .flush()
            .map_err(|e| StorageError::Backend(format!("Failed to flush database: {e}")))?;

        // Clear the log bounds cache to release any references
        self.log_bounds.write().await.clear();

        tracing::info!(
            "RocksDB storage shutdown complete, Arc strong count: {}",
            Arc::strong_count(&self.db)
        );
        Ok(())
    }

    // We could implement delete_all here if needed
    // async fn delete_all(&self) -> StorageResult<()> {
    //     // Implementation to clear all data
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_storage::LogStorageStreaming;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

    // Helper function to create LogIndex
    fn nz(n: u64) -> LogIndex {
        LogIndex::new(n).expect("test indices should be non-zero")
    }

    #[tokio::test]
    async fn test_rocksdb_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
        let namespace = StorageNamespace::new("test");

        // Test append and read
        let entries = Arc::new(vec![
            Bytes::from("data 1"),
            Bytes::from("data 2"),
            Bytes::from("data 3"),
        ]);
        let last_seq = storage.append(&namespace, entries).await.unwrap();
        assert_eq!(last_seq, nz(3));

        let range = storage.read_range(&namespace, nz(1), nz(4)).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (nz(1), Bytes::from("data 1")));
        assert_eq!(range[1], (nz(2), Bytes::from("data 2")));
        assert_eq!(range[2], (nz(3), Bytes::from("data 3")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(3))));
    }

    #[tokio::test]
    async fn test_rocksdb_streaming() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
        let namespace = StorageNamespace::new("test");

        // Append entries
        let entries = Arc::new(vec![
            Bytes::from("data 1"),
            Bytes::from("data 2"),
            Bytes::from("data 3"),
            Bytes::from("data 4"),
            Bytes::from("data 5"),
        ]);
        storage.append(&namespace, entries).await.unwrap();

        // Test streaming with end bound
        let mut stream = storage
            .stream_range(&namespace, nz(2), Some(nz(4)))
            .await
            .unwrap();

        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item.unwrap());
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (nz(2), Bytes::from("data 2")));
        assert_eq!(results[1], (nz(3), Bytes::from("data 3")));

        // Test streaming without end bound
        let mut stream = storage.stream_range(&namespace, nz(3), None).await.unwrap();

        let mut results = Vec::new();
        while let Some(item) = stream.next().await {
            results.push(item.unwrap());
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (nz(3), Bytes::from("data 3")));
        assert_eq!(results[1], (nz(4), Bytes::from("data 4")));
        assert_eq!(results[2], (nz(5), Bytes::from("data 5")));

        // Test streaming empty namespace
        let empty_ns = StorageNamespace::new("empty");
        let mut stream = storage.stream_range(&empty_ns, nz(1), None).await.unwrap();

        let mut count = 0;
        while (stream.next().await).is_some() {
            count += 1;
        }
        assert_eq!(count, 0);
    }
}
