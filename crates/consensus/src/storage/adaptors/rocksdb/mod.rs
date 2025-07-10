//! RocksDB storage adaptor implementation

pub mod config;

use crate::storage::{
    adaptors::memory::MemoryIterator,
    traits::{SnapshotMetadata, SnapshotStorage, StorageEngine},
    types::{
        BatchOperation, StorageError, StorageKey, StorageNamespace, StorageResult, StorageValue,
        WriteBatch,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, ReadOptions, WriteBatch as RocksWriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ops::{Bound, RangeBounds},
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::debug;

pub use config::{RocksDBAdaptorConfig, to_rocksdb_compression};

/// RocksDB storage implementation
#[derive(Debug, Clone)]
pub struct RocksDBStorage {
    /// The RocksDB instance
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    /// Path to the database
    path: String,
    /// Storage configuration
    config: RocksDBAdaptorConfig,
    /// Snapshot metadata cache
    snapshot_metadata: Arc<RwLock<HashMap<String, SnapshotMetadata>>>,
}

impl RocksDBStorage {
    /// Create a new RocksDB storage instance
    pub async fn new(config: RocksDBAdaptorConfig) -> StorageResult<Self> {
        let path_str = config
            .path
            .to_str()
            .ok_or_else(|| StorageError::Backend("Invalid path".to_string()))?
            .to_string();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_max_background_jobs(config.max_background_jobs);
        db_opts.set_max_total_wal_size(config.max_total_wal_size);
        db_opts.set_keep_log_file_num(config.keep_log_file_num);

        if config.enable_statistics {
            db_opts.enable_statistics();
        }

        if config.use_direct_io_for_flush_and_compaction {
            db_opts.set_use_direct_io_for_flush_and_compaction(true);
        }

        if config.enable_pipelined_write {
            db_opts.set_enable_pipelined_write(true);
        }

        db_opts.increase_parallelism(config.increase_parallelism);

        // Apply configuration
        if config.sync_writes {
            db_opts.set_use_fsync(true);
        }

        if config.disable_wal {
            // RocksDB doesn't have a direct disable_wal method, skip for now
            // TODO: Use options to disable WAL if needed
        }

        // Setup block cache
        let cache = rocksdb::Cache::new_lru_cache(config.block_cache_size);
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_block_size(config.block_size);

        if config.bloom_filter_bits > 0 {
            block_opts.set_bloom_filter(config.bloom_filter_bits as f64, true);
        }

        db_opts.set_block_based_table_factory(&block_opts);

        // Default column families
        let cf_names = vec![
            crate::storage::types::namespaces::DEFAULT,
            crate::storage::types::namespaces::LOGS,
            crate::storage::types::namespaces::METADATA,
            crate::storage::types::namespaces::SNAPSHOTS,
        ];

        let cfs: Vec<ColumnFamilyDescriptor> = cf_names
            .into_iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_compression_type(to_rocksdb_compression(config.compression.clone()));
                cf_opts.set_write_buffer_size(config.write_buffer_size);
                cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
                cf_opts.set_level_zero_file_num_compaction_trigger(
                    config.level0_file_num_compaction_trigger,
                );
                cf_opts
                    .set_level_zero_slowdown_writes_trigger(config.level0_slowdown_writes_trigger);
                cf_opts.set_level_zero_stop_writes_trigger(config.level0_stop_writes_trigger);
                cf_opts.set_target_file_size_base(config.target_file_size_base);
                cf_opts.set_target_file_size_multiplier(config.target_file_size_multiplier);
                ColumnFamilyDescriptor::new(name, cf_opts)
            })
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, &path_str, cfs)
            .map_err(|e| StorageError::Backend(format!("Failed to open RocksDB: {}", e)))?;

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            config,
            snapshot_metadata: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get a column family handle
    fn get_cf(&self, namespace: &StorageNamespace) -> StorageResult<Arc<BoundColumnFamily>> {
        self.db
            .cf_handle(namespace.as_str())
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))
    }
}

#[async_trait]
impl StorageEngine for RocksDBStorage {
    type Iterator = MemoryIterator;

    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>> {
        let cf = self.get_cf(namespace)?;
        self.db
            .get_cf(&cf, key.as_bytes())
            .map_err(|e| StorageError::Backend(e.to_string()))?
            .map(StorageValue::new)
            .map(Ok)
            .transpose()
    }

    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()> {
        let cf = self.get_cf(namespace)?;
        self.db
            .put_cf(&cf, key.as_bytes(), value.as_bytes())
            .map_err(|e| StorageError::Backend(e.to_string()))
    }

    async fn delete(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<()> {
        let cf = self.get_cf(namespace)?;
        self.db
            .delete_cf(&cf, key.as_bytes())
            .map_err(|e| StorageError::Backend(e.to_string()))
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        let mut rocks_batch = RocksWriteBatch::default();

        for op in batch.into_operations() {
            match op {
                BatchOperation::Put {
                    namespace,
                    key,
                    value,
                } => {
                    let cf = self.get_cf(&namespace)?;
                    rocks_batch.put_cf(&cf, key.as_bytes(), value.as_bytes());
                }
                BatchOperation::Delete { namespace, key } => {
                    let cf = self.get_cf(&namespace)?;
                    rocks_batch.delete_cf(&cf, key.as_bytes());
                }
            }
        }

        self.db
            .write(rocks_batch)
            .map_err(|e| StorageError::BatchFailed(e.to_string()))
    }

    async fn iter(&self, namespace: &StorageNamespace) -> StorageResult<Self::Iterator> {
        // For now, we'll collect all items into memory
        // A more sophisticated implementation would use a custom iterator wrapper
        let cf = self.get_cf(namespace)?;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        let mut items = Vec::new();
        for item in iter {
            match item {
                Ok((key, value)) => items.push((StorageKey::new(key), StorageValue::new(value))),
                Err(e) => return Err(StorageError::Backend(e.to_string())),
            }
        }

        Ok(MemoryIterator::new(items))
    }

    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator> {
        // Similar approach - collect matching items
        let cf = self.get_cf(namespace)?;

        let mode = match range.start_bound() {
            Bound::Unbounded => IteratorMode::Start,
            Bound::Included(start) => IteratorMode::From(start.as_bytes(), Direction::Forward),
            Bound::Excluded(start) => IteratorMode::From(start.as_bytes(), Direction::Forward),
        };

        let iter = self.db.iterator_cf(&cf, mode);
        let mut items = Vec::new();

        for item in iter {
            match item {
                Ok((key, value)) => {
                    let storage_key = StorageKey::new(key);

                    // Check start bound
                    match range.start_bound() {
                        Bound::Excluded(start) if &storage_key == start => continue,
                        _ => {}
                    }

                    // Check end bound
                    match range.end_bound() {
                        Bound::Included(end) if &storage_key > end => break,
                        Bound::Excluded(end) if &storage_key >= end => break,
                        _ => {}
                    }

                    items.push((storage_key, StorageValue::new(value)));
                }
                Err(e) => return Err(StorageError::Backend(e.to_string())),
            }
        }

        Ok(MemoryIterator::new(items))
    }

    async fn create_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(to_rocksdb_compression(self.config.compression.clone()));

        self.db
            .create_cf(namespace.as_str(), &cf_opts)
            .map_err(|e| StorageError::Backend(format!("Failed to create CF: {}", e)))?;

        debug!("Created namespace: {}", namespace);
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        self.db
            .drop_cf(namespace.as_str())
            .map_err(|e| StorageError::Backend(format!("Failed to drop CF: {}", e)))?;

        debug!("Dropped namespace: {}", namespace);
        Ok(())
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<StorageNamespace>> {
        // RocksDB doesn't provide a direct API to list CFs on an open DB
        // We'll need to re-open to get the list
        let cf_names = DBWithThreadMode::<MultiThreaded>::list_cf(&Options::default(), &self.path)
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        Ok(cf_names
            .into_iter()
            .filter(|name| name != "default") // Skip the default CF
            .map(StorageNamespace::new)
            .collect())
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db
            .flush()
            .map_err(|e| StorageError::Backend(e.to_string()))
    }

    async fn namespace_size(&self, namespace: &StorageNamespace) -> StorageResult<u64> {
        let cf = self.get_cf(namespace)?;

        // Get approximate size using properties
        let size_str = self
            .db
            .property_value_cf(&cf, "rocksdb.estimate-live-data-size")
            .map_err(|e| StorageError::Backend(e.to_string()))?
            .unwrap_or_else(|| "0".to_string());

        size_str
            .parse::<u64>()
            .map_err(|e| StorageError::Backend(format!("Failed to parse size: {}", e)))
    }
}

// Optimized LogStorage implementation for RocksDB with advanced features
#[async_trait]
impl<M> crate::storage::log::LogStorage<M> for RocksDBStorage
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    async fn append_entry(
        &self,
        namespace: &StorageNamespace,
        entry: crate::storage::log::LogEntry<M>,
    ) -> StorageResult<()> {
        use crate::storage::log::keys;

        let cf = self.get_cf(namespace)?;
        let mut batch = RocksWriteBatch::default();

        // Encode and store the entry
        let key = keys::encode_log_key(entry.index);
        let mut buffer = Vec::new();
        ciborium::into_writer(&entry, &mut buffer)
            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
        batch.put_cf(&cf, key.as_bytes(), &buffer);

        // Update metadata for O(1) access
        let metadata_key = keys::encode_metadata_key("log_state");
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state = state_opt.unwrap_or(crate::storage::log::LogState {
            first_index: entry.index,
            last_index: entry.index,
            entry_count: 0,
            total_bytes: 0,
        });

        let new_state = crate::storage::log::LogState {
            first_index: state.first_index.min(entry.index),
            last_index: state.last_index.max(entry.index),
            entry_count: state.entry_count + 1,
            total_bytes: state.total_bytes + entry.data.len() as u64,
        };

        let mut state_buffer = Vec::new();
        ciborium::into_writer(&new_state, &mut state_buffer)
            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
        batch.put_cf(&cf, metadata_key.as_bytes(), &state_buffer);

        // Use write options for durability
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Async writes for better performance

        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn append_entries(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<crate::storage::log::LogEntry<M>>,
    ) -> StorageResult<()> {
        use crate::storage::log::keys;

        if entries.is_empty() {
            return Ok(());
        }

        let cf = self.get_cf(namespace)?;
        let mut batch = RocksWriteBatch::default();
        let mut total_bytes = 0u64;
        let mut min_index = u64::MAX;
        let mut max_index = 0u64;

        // Batch all entries
        for entry in &entries {
            min_index = min_index.min(entry.index);
            max_index = max_index.max(entry.index);
            total_bytes += entry.data.len() as u64;

            let key = keys::encode_log_key(entry.index);
            let mut buffer = Vec::new();
            ciborium::into_writer(&entry, &mut buffer)
                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
            batch.put_cf(&cf, key.as_bytes(), &buffer);
        }

        // Update metadata once
        let metadata_key = keys::encode_metadata_key("log_state");
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state = state_opt.unwrap_or(crate::storage::log::LogState {
            first_index: min_index,
            last_index: max_index,
            entry_count: 0,
            total_bytes: 0,
        });

        let new_state = crate::storage::log::LogState {
            first_index: state.first_index.min(min_index),
            last_index: state.last_index.max(max_index),
            entry_count: state.entry_count + entries.len() as u64,
            total_bytes: state.total_bytes + total_bytes,
        };

        let mut state_buffer = Vec::new();
        ciborium::into_writer(&new_state, &mut state_buffer)
            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
        batch.put_cf(&cf, metadata_key.as_bytes(), &state_buffer);

        // Use write options for batch performance
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(false); // Async for batch operations
        write_opts.disable_wal(false); // Keep WAL for durability

        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn create_batch(&self) -> Box<dyn crate::storage::log::LogBatch<M>> {
        Box::new(RocksDBLogBatch::<M>::new())
    }

    async fn apply_batch(
        &self,
        _batch: Box<dyn crate::storage::log::LogBatch<M>>,
    ) -> StorageResult<()> {
        // For RocksDB, we would need a concrete type to apply the batch
        unimplemented!("Batch operations require concrete types")
    }

    async fn read_range<R: RangeBounds<u64> + Send>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> StorageResult<Vec<crate::storage::log::LogEntry<M>>> {
        use crate::storage::log::keys;

        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n + 1),
            std::ops::Bound::Excluded(&n) => Some(n),
            std::ops::Bound::Unbounded => None,
        };

        let cf = self.get_cf(namespace)?;
        let mut entries = Vec::new();

        // Use binary key encoding for efficient range scan
        let start_key = keys::log_range_start(start);

        // Configure iterator options for better performance
        let mut read_opts = ReadOptions::default();
        read_opts.set_prefix_same_as_start(true); // Optimize for log prefix
        read_opts.fill_cache(true); // Cache blocks for future reads

        let iter = if let Some(end_idx) = end {
            let end_key = keys::log_range_end(Some(end_idx));
            read_opts.set_iterate_upper_bound(end_key.as_bytes());
            self.db.iterator_cf_opt(
                &cf,
                read_opts,
                IteratorMode::From(start_key.as_bytes(), Direction::Forward),
            )
        } else {
            self.db.iterator_cf_opt(
                &cf,
                read_opts,
                IteratorMode::From(start_key.as_bytes(), Direction::Forward),
            )
        };

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::Backend(e.to_string()))?;
            let storage_key = StorageKey(Bytes::copy_from_slice(&key));

            if let Some(index) = keys::decode_log_index(&storage_key) {
                if let Some(end_idx) = end {
                    if index >= end_idx {
                        break;
                    }
                }

                let entry: crate::storage::log::LogEntry<M> = ciborium::from_reader(&value[..])
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    async fn get_entry(
        &self,
        namespace: &StorageNamespace,
        index: u64,
    ) -> StorageResult<Option<crate::storage::log::LogEntry<M>>> {
        use crate::storage::log::keys;

        let cf = self.get_cf(namespace)?;
        let key = keys::encode_log_key(index);

        // Use read options for consistency
        let mut read_opts = ReadOptions::default();
        read_opts.fill_cache(true);

        match self.db.get_cf_opt(&cf, key.as_bytes(), &read_opts) {
            Ok(Some(value)) => {
                let entry: crate::storage::log::LogEntry<M> = ciborium::from_reader(&value[..])
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Backend(e.to_string())),
        }
    }

    async fn get_last_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<crate::storage::log::LogEntry<M>>> {
        // O(1) lookup using cached metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(state) = state_opt {
            self.get_entry(namespace, state.last_index).await
        } else {
            Ok(None)
        }
    }

    async fn get_first_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<crate::storage::log::LogEntry<M>>> {
        // O(1) lookup using cached metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(state) = state_opt {
            self.get_entry(namespace, state.first_index).await
        } else {
            Ok(None)
        }
    }

    async fn truncate(&self, namespace: &StorageNamespace, after_index: u64) -> StorageResult<()> {
        use crate::storage::log::keys;

        let cf = self.get_cf(namespace)?;
        let mut batch = RocksWriteBatch::default();

        // Get current state to update metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(mut state) = state_opt {
            // Use DeleteRange for efficient bulk deletion
            let start_key = keys::log_range_start(after_index + 1);
            let end_key = keys::log_range_end(None);

            // Count entries to be deleted for metadata update
            let mut deleted_count = 0u64;
            let mut deleted_bytes = 0u64;

            let iter = self.db.iterator_cf(
                &cf,
                IteratorMode::From(start_key.as_bytes(), Direction::Forward),
            );

            for item in iter {
                let (key, value) = item.map_err(|e| StorageError::Backend(e.to_string()))?;
                let storage_key = StorageKey(Bytes::copy_from_slice(&key));

                if keys::decode_log_index(&storage_key).is_some() {
                    deleted_count += 1;
                    let entry: crate::storage::log::LogEntry<M> = ciborium::from_reader(&value[..])
                        .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                    deleted_bytes += entry.data.len() as u64;
                }
            }

            // Use DeleteRange for efficient deletion
            batch.delete_range_cf(&cf, start_key.as_bytes(), end_key.as_bytes());

            // Update metadata
            if deleted_count > 0 {
                state.last_index = after_index;
                state.entry_count -= deleted_count;
                state.total_bytes -= deleted_bytes;

                let metadata_key = keys::encode_metadata_key("log_state");
                let mut state_buffer = Vec::new();
                ciborium::into_writer(&state, &mut state_buffer)
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                batch.put_cf(&cf, metadata_key.as_bytes(), &state_buffer);
            }
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn purge(&self, namespace: &StorageNamespace, up_to_index: u64) -> StorageResult<()> {
        use crate::storage::log::keys;

        let cf = self.get_cf(namespace)?;
        let mut batch = RocksWriteBatch::default();

        // Get current state to update metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(mut state) = state_opt {
            // Use DeleteRange for efficient bulk deletion
            let start_key = keys::log_range_start(0);
            let end_key = keys::log_range_end(Some(up_to_index + 1));

            // Count entries to be deleted for metadata update
            let mut deleted_count = 0u64;
            let mut deleted_bytes = 0u64;

            let mut read_opts = ReadOptions::default();
            read_opts.set_iterate_upper_bound(end_key.as_bytes());

            let iter = self.db.iterator_cf_opt(
                &cf,
                read_opts,
                IteratorMode::From(start_key.as_bytes(), Direction::Forward),
            );

            for item in iter {
                let (key, value) = item.map_err(|e| StorageError::Backend(e.to_string()))?;
                let storage_key = StorageKey(Bytes::copy_from_slice(&key));

                if let Some(index) = keys::decode_log_index(&storage_key) {
                    if index <= up_to_index {
                        deleted_count += 1;
                        let entry: crate::storage::log::LogEntry<M> =
                            ciborium::from_reader(&value[..])
                                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                        deleted_bytes += entry.data.len() as u64;
                    }
                }
            }

            // Use DeleteRange for efficient deletion
            batch.delete_range_cf(&cf, start_key.as_bytes(), end_key.as_bytes());

            // Update metadata
            if deleted_count > 0 {
                state.first_index = up_to_index + 1;
                state.entry_count -= deleted_count;
                state.total_bytes -= deleted_bytes;

                let metadata_key = keys::encode_metadata_key("log_state");
                let mut state_buffer = Vec::new();
                ciborium::into_writer(&state, &mut state_buffer)
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                batch.put_cf(&cf, metadata_key.as_bytes(), &state_buffer);
            }
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get_log_state(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<crate::storage::log::LogState>> {
        use crate::storage::log::keys;

        let cf = self.get_cf(namespace)?;
        let metadata_key = keys::encode_metadata_key("log_state");

        match self.db.get_cf(&cf, metadata_key.as_bytes()) {
            Ok(Some(value)) => {
                let state: crate::storage::log::LogState = ciborium::from_reader(&value[..])
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                Ok(Some(state))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::Backend(e.to_string())),
        }
    }

    async fn read_time_range(
        &self,
        namespace: &StorageNamespace,
        start_time: u64,
        end_time: u64,
    ) -> StorageResult<Vec<crate::storage::log::LogEntry<M>>> {
        // For time-based queries, we need to scan entries
        // A more efficient implementation would maintain a time-based secondary index
        let entries = self.read_range(namespace, ..).await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.timestamp >= start_time && e.timestamp <= end_time)
            .collect())
    }

    async fn compact(
        &self,
        namespace: &StorageNamespace,
        up_to_index: u64,
    ) -> StorageResult<crate::storage::log::CompactionResult> {
        // Get state before compaction
        let state_before_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state_before = state_before_opt.unwrap_or(crate::storage::log::LogState {
            first_index: 0,
            last_index: 0,
            entry_count: 0,
            total_bytes: 0,
        });

        // Purge old entries
        <Self as crate::storage::log::LogStorage<M>>::purge(self, namespace, up_to_index).await?;

        // Trigger RocksDB compaction for space reclamation
        let cf = self.get_cf(namespace)?;
        self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);

        // Get state after compaction
        let state_after_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state_after = state_after_opt.unwrap_or(crate::storage::log::LogState {
            first_index: 0,
            last_index: 0,
            entry_count: 0,
            total_bytes: 0,
        });

        Ok(crate::storage::log::CompactionResult {
            entries_compacted: state_before.entry_count - state_after.entry_count,
            bytes_freed: state_before.total_bytes - state_after.total_bytes,
            new_first_index: state_after.first_index,
        })
    }
}

/// RocksDB-based log batch implementation
struct RocksDBLogBatch<M> {
    operations: Vec<LogBatchOp<M>>,
    _marker: std::marker::PhantomData<M>,
}

#[allow(dead_code)]
enum LogBatchOp<M> {
    Append(StorageNamespace, crate::storage::log::LogEntry<M>),
    Truncate(StorageNamespace, u64),
    Purge(StorageNamespace, u64),
}

impl<M> RocksDBLogBatch<M> {
    fn new() -> Self {
        Self {
            operations: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<M> crate::storage::log::LogBatch<M> for RocksDBLogBatch<M>
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn append(&mut self, namespace: StorageNamespace, entry: crate::storage::log::LogEntry<M>) {
        self.operations.push(LogBatchOp::Append(namespace, entry));
    }

    fn truncate(&mut self, namespace: StorageNamespace, after_index: u64) {
        self.operations
            .push(LogBatchOp::Truncate(namespace, after_index));
    }

    fn purge(&mut self, namespace: StorageNamespace, up_to_index: u64) {
        self.operations
            .push(LogBatchOp::Purge(namespace, up_to_index));
    }
}

#[async_trait]
impl SnapshotStorage for RocksDBStorage {
    async fn create_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<Bytes> {
        // Create a checkpoint of the namespace
        let _checkpoint_path = format!("{}/snapshots/{}/{}", self.path, namespace, snapshot_id);

        // For simplicity, we'll collect all data from the namespace
        let cf = self.get_cf(namespace)?;
        let mut data = Vec::new();

        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        for item in iter {
            match item {
                Ok((key, value)) => data.push((key.to_vec(), value.to_vec())),
                Err(e) => return Err(StorageError::SnapshotError(e.to_string())),
            }
        }

        // Serialize the data
        let snapshot_data =
            serde_json::to_vec(&data).map_err(|e| StorageError::SnapshotError(e.to_string()))?;

        // Store metadata
        let metadata = SnapshotMetadata {
            id: snapshot_id.to_string(),
            size: snapshot_data.len() as u64,
            created_at: chrono::Utc::now().timestamp() as u64,
            namespace: namespace.clone(),
        };

        self.snapshot_metadata
            .write()
            .await
            .insert(format!("{}:{}", namespace, snapshot_id), metadata);

        Ok(Bytes::from(snapshot_data))
    }

    async fn restore_snapshot(
        &self,
        namespace: &StorageNamespace,
        _snapshot_id: &str,
        data: Bytes,
    ) -> StorageResult<()> {
        let cf = self.get_cf(namespace)?;

        // Deserialize the data
        let items: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(&data)
            .map_err(|e| StorageError::SnapshotError(e.to_string()))?;

        // Clear existing data
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        let mut batch = RocksWriteBatch::default();
        for item in iter {
            match item {
                Ok((key, _)) => batch.delete_cf(&cf, key),
                Err(e) => return Err(StorageError::SnapshotError(e.to_string())),
            }
        }

        // Restore data
        for (key, value) in items {
            batch.put_cf(&cf, key, value);
        }

        self.db
            .write(batch)
            .map_err(|e| StorageError::SnapshotError(e.to_string()))?;

        Ok(())
    }

    async fn list_snapshots(&self, namespace: &StorageNamespace) -> StorageResult<Vec<String>> {
        let metadata = self.snapshot_metadata.read().await;
        let prefix = format!("{}:", namespace);

        Ok(metadata
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .map(|k| k[prefix.len()..].to_string())
            .collect())
    }

    async fn delete_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<()> {
        self.snapshot_metadata
            .write()
            .await
            .remove(&format!("{}:{}", namespace, snapshot_id));
        Ok(())
    }

    async fn snapshot_metadata(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<SnapshotMetadata> {
        self.snapshot_metadata
            .read()
            .await
            .get(&format!("{}:{}", namespace, snapshot_id))
            .cloned()
            .ok_or_else(|| {
                StorageError::SnapshotError(format!("Snapshot {} not found", snapshot_id))
            })
    }
}
