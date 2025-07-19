//! RocksDB log storage implementation

pub mod config;

use async_trait::async_trait;
use bytes::Bytes;
use proven_storage::{LogStorage, StorageAdaptor, StorageError, StorageNamespace, StorageResult};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch,
};
use std::{collections::HashMap, path::Path, sync::Arc};
use tokio::sync::RwLock;

/// RocksDB log storage implementation
#[derive(Clone)]
pub struct RocksDbStorage {
    /// The RocksDB instance
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    /// Log bounds cache: namespace -> (first_index, last_index)
    log_bounds: Arc<RwLock<HashMap<StorageNamespace, (u64, u64)>>>,
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

    /// Encode a log key for RocksDB
    fn encode_key(index: u64) -> Vec<u8> {
        index.to_be_bytes().to_vec()
    }

    /// Decode a log key from RocksDB
    fn decode_key(key: &[u8]) -> StorageResult<u64> {
        if key.len() != 8 {
            return Err(StorageError::InvalidKey("Invalid key length".to_string()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(u64::from_be_bytes(bytes))
    }
}

#[async_trait]
impl LogStorage for RocksDbStorage {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(u64, Bytes)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        // Get current bounds
        let (mut first_index, mut last_index) =
            bounds.get(namespace).copied().unwrap_or((u64::MAX, 0));

        // Add all entries to batch
        for (index, data) in entries {
            batch.put_cf(&cf, Self::encode_key(index), data.as_ref());

            // Update bounds
            if first_index == u64::MAX || index < first_index {
                first_index = index;
            }
            if index > last_index {
                last_index = index;
            }
        }

        // Write batch
        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache
        bounds.insert(namespace.clone(), (first_index, last_index));

        Ok(())
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: u64,
    ) -> StorageResult<Vec<(u64, Bytes)>> {
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

    async fn truncate_after(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        let start_key = Self::encode_key(index + 1);
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for result in iter {
            let (key, _) =
                result.map_err(|e| StorageError::Backend(format!("Iterator error: {e}")))?;
            batch.delete_cf(&cf, key);
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

    async fn compact_before(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        let cf = self.get_or_create_cf(namespace)?;
        let mut batch = WriteBatch::default();
        let mut bounds = self.log_bounds.write().await;

        let end_key = Self::encode_key(index + 1);
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for result in iter {
            let (key, _) =
                result.map_err(|e| StorageError::Backend(format!("Iterator error: {e}")))?;

            if key.as_ref() >= end_key.as_slice() {
                break;
            }

            batch.delete_cf(&cf, key);
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(format!("Failed to write batch: {e}")))?;

        // Update bounds cache
        if let Some((first, last)) = bounds.get_mut(namespace) {
            if index >= *first {
                *first = index + 1;
            }
            if *first > *last {
                bounds.remove(namespace);
            }
        }

        Ok(())
    }

    async fn bounds(&self, namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>> {
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
}

// Implement LogStorageWithDelete for RocksDbStorage
#[async_trait]
impl proven_storage::LogStorageWithDelete for RocksDbStorage {
    async fn delete_entry(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<bool> {
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
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_rocksdb_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::new(temp_dir.path()).await.unwrap();
        let namespace = StorageNamespace::new("test");

        // Test append and read
        let entries = vec![
            (1, Bytes::from("data 1")),
            (2, Bytes::from("data 2")),
            (3, Bytes::from("data 3")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        let range = storage.read_range(&namespace, 1, 4).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (1, Bytes::from("data 1")));
        assert_eq!(range[1], (2, Bytes::from("data 2")));
        assert_eq!(range[2], (3, Bytes::from("data 3")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((1, 3)));
    }
}
