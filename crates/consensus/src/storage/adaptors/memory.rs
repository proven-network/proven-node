//! In-memory storage adaptor implementation

use crate::storage::{
    traits::{SnapshotMetadata, SnapshotStorage, StorageEngine},
    types::{
        BatchOperation, StorageError, StorageIterator, StorageKey, StorageNamespace, StorageResult,
        StorageValue, WriteBatch,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeBounds,
    sync::Arc,
};
use tokio::sync::RwLock;

/// In-memory storage implementation using BTreeMap for ordering
#[derive(Clone, Debug)]
pub struct MemoryStorage {
    /// Namespaces mapping to their key-value stores
    namespaces: Arc<RwLock<HashMap<StorageNamespace, BTreeMap<StorageKey, StorageValue>>>>,
    /// Snapshots stored by namespace and ID
    snapshots: Arc<RwLock<HashMap<StorageNamespace, HashMap<String, Bytes>>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage instance
    pub fn new() -> Self {
        let mut namespaces = HashMap::new();

        // Pre-create common namespaces
        namespaces.insert(
            StorageNamespace::new(crate::storage::types::namespaces::DEFAULT),
            BTreeMap::new(),
        );
        namespaces.insert(
            StorageNamespace::new(crate::storage::types::namespaces::LOGS),
            BTreeMap::new(),
        );
        namespaces.insert(
            StorageNamespace::new(crate::storage::types::namespaces::METADATA),
            BTreeMap::new(),
        );
        namespaces.insert(
            StorageNamespace::new(crate::storage::types::namespaces::SNAPSHOTS),
            BTreeMap::new(),
        );

        Self {
            namespaces: Arc::new(RwLock::new(namespaces)),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create a namespace
    async fn get_or_create_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        let mut namespaces = self.namespaces.write().await;
        namespaces
            .entry(namespace.clone())
            .or_insert_with(BTreeMap::new);
        Ok(())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator implementation for memory storage
pub struct MemoryIterator {
    items: Vec<(StorageKey, StorageValue)>,
    position: usize,
}

impl MemoryIterator {
    pub(crate) fn new(items: Vec<(StorageKey, StorageValue)>) -> Self {
        Self { items, position: 0 }
    }
}

impl StorageIterator for MemoryIterator {
    fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>> {
        if self.position < self.items.len() {
            let item = self.items[self.position].clone();
            self.position += 1;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    fn seek(&mut self, key: &StorageKey) -> StorageResult<()> {
        // Find the first item >= key
        self.position = self
            .items
            .iter()
            .position(|(k, _)| k >= key)
            .unwrap_or(self.items.len());
        Ok(())
    }

    fn valid(&self) -> bool {
        self.position < self.items.len()
    }
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    type Iterator = MemoryIterator;

    async fn get(
        &self,
        namespace: &StorageNamespace,
        key: &StorageKey,
    ) -> StorageResult<Option<StorageValue>> {
        let namespaces = self.namespaces.read().await;
        Ok(namespaces
            .get(namespace)
            .and_then(|store| store.get(key))
            .cloned())
    }

    async fn put(
        &self,
        namespace: &StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> StorageResult<()> {
        self.get_or_create_namespace(namespace).await?;
        let mut namespaces = self.namespaces.write().await;
        if let Some(store) = namespaces.get_mut(namespace) {
            store.insert(key, value);
        }
        Ok(())
    }

    async fn delete(&self, namespace: &StorageNamespace, key: &StorageKey) -> StorageResult<()> {
        let mut namespaces = self.namespaces.write().await;
        if let Some(store) = namespaces.get_mut(namespace) {
            store.remove(key);
        }
        Ok(())
    }

    async fn write_batch(&self, batch: WriteBatch) -> StorageResult<()> {
        let mut namespaces = self.namespaces.write().await;

        for op in batch.into_operations() {
            match op {
                BatchOperation::Put {
                    namespace,
                    key,
                    value,
                } => {
                    let store = namespaces.entry(namespace).or_insert_with(BTreeMap::new);
                    store.insert(key, value);
                }
                BatchOperation::Delete { namespace, key } => {
                    if let Some(store) = namespaces.get_mut(&namespace) {
                        store.remove(&key);
                    }
                }
            }
        }

        Ok(())
    }

    async fn iter(&self, namespace: &StorageNamespace) -> StorageResult<Self::Iterator> {
        let namespaces = self.namespaces.read().await;
        let items = namespaces
            .get(namespace)
            .map(|store| store.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        Ok(MemoryIterator::new(items))
    }

    async fn iter_range(
        &self,
        namespace: &StorageNamespace,
        range: impl RangeBounds<StorageKey> + Send,
    ) -> StorageResult<Self::Iterator> {
        let namespaces = self.namespaces.read().await;
        let items = namespaces
            .get(namespace)
            .map(|store| {
                store
                    .range(range)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Ok(MemoryIterator::new(items))
    }

    async fn create_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        let mut namespaces = self.namespaces.write().await;
        namespaces.insert(namespace.clone(), BTreeMap::new());
        Ok(())
    }

    async fn drop_namespace(&self, namespace: &StorageNamespace) -> StorageResult<()> {
        let mut namespaces = self.namespaces.write().await;
        namespaces.remove(namespace);

        // Also remove any snapshots for this namespace
        let mut snapshots = self.snapshots.write().await;
        snapshots.remove(namespace);

        Ok(())
    }

    async fn list_namespaces(&self) -> StorageResult<Vec<StorageNamespace>> {
        let namespaces = self.namespaces.read().await;
        Ok(namespaces.keys().cloned().collect())
    }

    async fn flush(&self) -> StorageResult<()> {
        // No-op for memory storage
        Ok(())
    }

    async fn namespace_size(&self, namespace: &StorageNamespace) -> StorageResult<u64> {
        let namespaces = self.namespaces.read().await;
        let size = namespaces
            .get(namespace)
            .map(|store| {
                store
                    .iter()
                    .map(|(k, v)| k.as_bytes().len() + v.as_bytes().len())
                    .sum::<usize>() as u64
            })
            .unwrap_or(0);
        Ok(size)
    }
}

// NOTE: LogStorage implementation has been removed as it's now generic
// and should be implemented by consumers with specific metadata types.
/// Memory-based log batch implementation
struct MemoryLogBatch<M> {
    inner: WriteBatch,
    _marker: std::marker::PhantomData<M>,
}

impl<M> MemoryLogBatch<M> {
    fn new() -> Self {
        Self {
            inner: WriteBatch::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<M> crate::storage::log::LogBatch<M> for MemoryLogBatch<M>
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    fn append(&mut self, namespace: StorageNamespace, entry: crate::storage::log::LogEntry<M>) {
        use crate::storage::log::keys;

        let key = keys::encode_log_key(entry.index);
        let mut buffer = Vec::new();
        // Ignore serialization errors in batch operations
        if let Ok(()) = ciborium::into_writer(&entry, &mut buffer) {
            self.inner.put(namespace, key, StorageValue::new(buffer));
        }
    }

    fn truncate(&mut self, namespace: StorageNamespace, after_index: u64) {
        // Batch truncate not implemented for memory storage
        // Would need to track keys to delete
        let _ = (namespace, after_index);
    }

    fn purge(&mut self, namespace: StorageNamespace, up_to_index: u64) {
        // Batch purge not implemented for memory storage
        // Would need to track keys to delete
        let _ = (namespace, up_to_index);
    }
}

// Optimized LogStorage implementation for in-memory storage
#[async_trait]
impl<M> crate::storage::log::LogStorage<M> for MemoryStorage
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    async fn append_entry(
        &self,
        namespace: &StorageNamespace,
        entry: crate::storage::log::LogEntry<M>,
    ) -> StorageResult<()> {
        use crate::storage::log::keys;

        // Update metadata for O(1) access
        let mut batch = WriteBatch::new();

        // Encode and store the entry
        let key = keys::encode_log_key(entry.index);
        let mut buffer = Vec::new();
        ciborium::into_writer(&entry, &mut buffer)
            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
        batch.put(namespace.clone(), key, StorageValue::new(buffer));

        // Update metadata
        let metadata_key = keys::encode_metadata_key("log_state");
        let state: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state = state.unwrap_or(crate::storage::log::LogState {
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
        batch.put(
            namespace.clone(),
            metadata_key,
            StorageValue::new(state_buffer),
        );

        self.write_batch(batch).await
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

        let mut batch = WriteBatch::new();
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
            batch.put(namespace.clone(), key, StorageValue::new(buffer));
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
        batch.put(
            namespace.clone(),
            metadata_key,
            StorageValue::new(state_buffer),
        );

        self.write_batch(batch).await
    }

    async fn create_batch(&self) -> Box<dyn crate::storage::log::LogBatch<M>> {
        Box::new(MemoryLogBatch::<M>::new())
    }

    async fn apply_batch(
        &self,
        _batch: Box<dyn crate::storage::log::LogBatch<M>>,
    ) -> StorageResult<()> {
        // For memory storage, we can't downcast a trait object, so we'll need a different approach
        // This would need to be implemented with a concrete type
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

        // Use binary key encoding for efficient range scan
        let start_key = keys::log_range_start(start);
        let end_key = keys::log_range_end(end);

        let mut entries = Vec::new();
        let mut iter = self.iter_range(namespace, start_key..end_key).await?;

        while let Some((key, value)) = iter.next()? {
            if let Some(_index) = keys::decode_log_index(&key) {
                let entry: crate::storage::log::LogEntry<M> =
                    ciborium::from_reader(value.as_bytes())
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

        let key = keys::encode_log_key(index);
        if let Some(value) = self.get(namespace, &key).await? {
            let entry: crate::storage::log::LogEntry<M> =
                ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
            Ok(Some(entry))
        } else {
            Ok(None)
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

        let mut batch = WriteBatch::new();

        // Get current state to update metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(mut state) = state_opt {
            // Find entries to delete
            let start_key = keys::log_range_start(after_index + 1);
            let end_key = keys::log_range_end(None);

            let mut iter = self.iter_range(namespace, start_key..end_key).await?;
            let mut deleted_count = 0u64;
            let mut deleted_bytes = 0u64;

            while let Some((key, value)) = iter.next()? {
                if let Some(_index) = keys::decode_log_index(&key) {
                    deleted_count += 1;
                    let entry: crate::storage::log::LogEntry<M> =
                        ciborium::from_reader(value.as_bytes())
                            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                    deleted_bytes += entry.data.len() as u64;
                    batch.delete(namespace.clone(), key);
                }
            }

            // Update metadata
            if deleted_count > 0 {
                state.last_index = after_index;
                state.entry_count -= deleted_count;
                state.total_bytes -= deleted_bytes;

                let metadata_key = keys::encode_metadata_key("log_state");
                let mut state_buffer = Vec::new();
                ciborium::into_writer(&state, &mut state_buffer)
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                batch.put(
                    namespace.clone(),
                    metadata_key,
                    StorageValue::new(state_buffer),
                );
            }
        }

        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn purge(&self, namespace: &StorageNamespace, up_to_index: u64) -> StorageResult<()> {
        use crate::storage::log::keys;

        let mut batch = WriteBatch::new();

        // Get current state to update metadata
        let state_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        if let Some(mut state) = state_opt {
            // Find entries to delete
            let start_key = keys::log_range_start(0);
            let end_key = keys::log_range_end(Some(up_to_index + 1));

            let mut iter = self.iter_range(namespace, start_key..end_key).await?;
            let mut deleted_count = 0u64;
            let mut deleted_bytes = 0u64;

            while let Some((key, value)) = iter.next()? {
                if let Some(index) = keys::decode_log_index(&key) {
                    if index <= up_to_index {
                        deleted_count += 1;
                        let entry: crate::storage::log::LogEntry<M> =
                            ciborium::from_reader(value.as_bytes())
                                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                        deleted_bytes += entry.data.len() as u64;
                        batch.delete(namespace.clone(), key);
                    }
                }
            }

            // Update metadata
            if deleted_count > 0 {
                state.first_index = up_to_index + 1;
                state.entry_count -= deleted_count;
                state.total_bytes -= deleted_bytes;

                let metadata_key = keys::encode_metadata_key("log_state");
                let mut state_buffer = Vec::new();
                ciborium::into_writer(&state, &mut state_buffer)
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                batch.put(
                    namespace.clone(),
                    metadata_key,
                    StorageValue::new(state_buffer),
                );
            }
        }

        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn get_log_state(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<crate::storage::log::LogState>> {
        use crate::storage::log::keys;

        let metadata_key = keys::encode_metadata_key("log_state");
        if let Some(value) = self.get(namespace, &metadata_key).await? {
            let state: crate::storage::log::LogState = ciborium::from_reader(value.as_bytes())
                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    async fn read_time_range(
        &self,
        namespace: &StorageNamespace,
        start_time: u64,
        end_time: u64,
    ) -> StorageResult<Vec<crate::storage::log::LogEntry<M>>> {
        // For time-based queries, we need to scan all entries
        // In a real implementation, we might maintain a time-based index
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
        // For memory storage, compaction is just purging old entries
        let state_before_opt: Option<crate::storage::log::LogState> =
            <Self as crate::storage::log::LogStorage<M>>::get_log_state(self, namespace).await?;
        let state_before = state_before_opt.unwrap_or(crate::storage::log::LogState {
            first_index: 0,
            last_index: 0,
            entry_count: 0,
            total_bytes: 0,
        });

        <Self as crate::storage::log::LogStorage<M>>::purge(self, namespace, up_to_index).await?;

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

/* Removed - LogStorage is now generic and domain-specific
#[async_trait]
impl crate::storage::EnhancedLogStorage for MemoryStorage {
    async fn append_entry(
        &self,
        namespace: &StorageNamespace,
        entry: LogEntry,
    ) -> StorageResult<()> {
        let key = crate::storage::keys::encode_log_key(entry.index);
        let mut buffer = Vec::new();
        ciborium::into_writer(&entry, &mut buffer)
            .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
        let value = StorageValue::new(buffer);
        self.put(namespace, key, value).await
    }

    async fn append_entries(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<LogEntry>,
    ) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        for entry in entries {
            let key = crate::storage::keys::encode_log_key(entry.index);
            let mut buffer = Vec::new();
            ciborium::into_writer(&entry, &mut buffer)
                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
            let value = StorageValue::new(buffer);
            batch.put(namespace.clone(), key, value);
        }
        self.write_batch(batch).await
    }

    async fn read_range<R: RangeBounds<u64> + Send>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> StorageResult<Vec<LogEntry>> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces
            .get(namespace)
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))?;

        let mut entries = Vec::new();
        for (key, value) in store.iter() {
            if let Some(index) = crate::storage::keys::decode_log_index(key) {
                if range.contains(&index) {
                    let entry: LogEntry = ciborium::from_reader(value.as_bytes())
                        .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                    entries.push(entry);
                }
            }
        }

        // Sort by index since BTreeMap is sorted by key (which is encoded)
        entries.sort_by_key(|e| e.index);
        Ok(entries)
    }

    async fn get_entry(
        &self,
        namespace: &StorageNamespace,
        index: u64,
    ) -> StorageResult<Option<LogEntry>> {
        let key = crate::storage::keys::encode_log_key(index);
        if let Some(value) = self.get(namespace, &key).await? {
            let entry: LogEntry = ciborium::from_reader(value.as_bytes())
                .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn get_last_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry>> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces
            .get(namespace)
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))?;

        // Find the last log entry by iterating through keys
        let mut last_entry = None;
        let mut last_index = 0u64;

        for (key, value) in store.iter() {
            if let Some(index) = crate::storage::keys::decode_log_index(key) {
                if index >= last_index {
                    last_index = index;
                    let entry: LogEntry = ciborium::from_reader(value.as_bytes())
                        .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                    last_entry = Some(entry);
                }
            }
        }

        Ok(last_entry)
    }

    async fn get_first_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry>> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces
            .get(namespace)
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))?;

        // Find the first log entry
        let mut first_entry = None;
        let mut first_index = u64::MAX;

        for (key, value) in store.iter() {
            if let Some(index) = crate::storage::keys::decode_log_index(key) {
                if index < first_index {
                    first_index = index;
                    let entry: LogEntry = ciborium::from_reader(value.as_bytes())
                        .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                    first_entry = Some(entry);
                }
            }
        }

        Ok(first_entry)
    }

    async fn truncate(&self, namespace: &StorageNamespace, after_index: u64) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        let namespaces = self.namespaces.read().await;

        if let Some(store) = namespaces.get(namespace) {
            for key in store.keys() {
                if let Some(index) = crate::storage::keys::decode_log_index(key) {
                    if index > after_index {
                        batch.delete(namespace.clone(), key.clone());
                    }
                }
            }
        }

        drop(namespaces);
        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn purge(&self, namespace: &StorageNamespace, up_to_index: u64) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        let namespaces = self.namespaces.read().await;

        if let Some(store) = namespaces.get(namespace) {
            for key in store.keys() {
                if let Some(index) = crate::storage::keys::decode_log_index(key) {
                    if index <= up_to_index {
                        batch.delete(namespace.clone(), key.clone());
                    }
                }
            }
        }

        drop(namespaces);
        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    async fn get_log_state(&self, namespace: &StorageNamespace) -> StorageResult<Option<LogState>> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces.get(namespace);

        if let Some(store) = store {
            let mut first_index = u64::MAX;
            let mut last_index = 0u64;
            let mut entry_count = 0u64;
            let mut total_bytes = 0u64;

            for (key, value) in store.iter() {
                if let Some(index) = crate::storage::keys::decode_log_index(key) {
                    first_index = first_index.min(index);
                    last_index = last_index.max(index);
                    entry_count += 1;
                    total_bytes += value.as_bytes().len() as u64;
                }
            }

            if entry_count > 0 {
                Ok(Some(LogState {
                    first_index,
                    last_index,
                    entry_count,
                    total_bytes,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn read_time_range(
        &self,
        namespace: &StorageNamespace,
        start_time: u64,
        end_time: u64,
    ) -> StorageResult<Vec<LogEntry>> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces
            .get(namespace)
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))?;

        let mut entries = Vec::new();
        for (key, value) in store.iter() {
            if crate::storage::keys::decode_log_index(key).is_some() {
                let entry: LogEntry = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::InvalidValue(e.to_string()))?;
                if entry.timestamp >= start_time && entry.timestamp <= end_time {
                    entries.push(entry);
                }
            }
        }

        // Sort by timestamp
        entries.sort_by_key(|e| e.timestamp);
        Ok(entries)
    }

    async fn compact(
        &self,
        namespace: &StorageNamespace,
        up_to_index: u64,
    ) -> StorageResult<CompactionResult> {
        // For memory storage, compaction just means purging old entries
        let state_before = self.get_log_state(namespace).await?;
        <Self as crate::storage::log::LogStorage<M>>::purge(self, namespace, up_to_index).await?;
        let state_after = self.get_log_state(namespace).await?;

        let entries_compacted = state_before
            .map(|s| s.entry_count)
            .unwrap_or(0)
            .saturating_sub(state_after.map(|s| s.entry_count).unwrap_or(0));

        let bytes_freed = state_before
            .map(|s| s.total_bytes)
            .unwrap_or(0)
            .saturating_sub(state_after.map(|s| s.total_bytes).unwrap_or(0));

        Ok(CompactionResult {
            entries_compacted,
            bytes_freed,
            new_first_index: state_after
                .map(|s| s.first_index)
                .unwrap_or(up_to_index + 1),
        })
    }
}
*/

#[async_trait]
impl SnapshotStorage for MemoryStorage {
    async fn create_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<Bytes> {
        let namespaces = self.namespaces.read().await;
        let store = namespaces
            .get(namespace)
            .ok_or_else(|| StorageError::NamespaceNotFound(namespace.to_string()))?;

        // Simple snapshot: serialize the BTreeMap as JSON
        let snapshot_data = serde_json::to_vec(
            &store
                .iter()
                .map(|(k, v)| (hex::encode(k.as_bytes()), hex::encode(v.as_bytes())))
                .collect::<Vec<_>>(),
        )
        .map_err(|e| StorageError::SnapshotError(e.to_string()))?;

        let snapshot_bytes = Bytes::from(snapshot_data);

        // Store the snapshot
        let mut snapshots = self.snapshots.write().await;
        let namespace_snapshots = snapshots
            .entry(namespace.clone())
            .or_insert_with(HashMap::new);
        namespace_snapshots.insert(snapshot_id.to_string(), snapshot_bytes.clone());

        Ok(snapshot_bytes)
    }

    async fn restore_snapshot(
        &self,
        namespace: &StorageNamespace,
        _snapshot_id: &str,
        data: Bytes,
    ) -> StorageResult<()> {
        // Deserialize the snapshot data
        let items: Vec<(String, String)> = serde_json::from_slice(&data)
            .map_err(|e| StorageError::SnapshotError(e.to_string()))?;

        let mut namespaces = self.namespaces.write().await;
        let store = namespaces
            .entry(namespace.clone())
            .or_insert_with(BTreeMap::new);

        // Clear existing data and restore from snapshot
        store.clear();
        for (key_hex, value_hex) in items {
            let key = StorageKey::new(
                hex::decode(key_hex).map_err(|e| StorageError::SnapshotError(e.to_string()))?,
            );
            let value = StorageValue::new(
                hex::decode(value_hex).map_err(|e| StorageError::SnapshotError(e.to_string()))?,
            );
            store.insert(key, value);
        }

        Ok(())
    }

    async fn list_snapshots(&self, namespace: &StorageNamespace) -> StorageResult<Vec<String>> {
        let snapshots = self.snapshots.read().await;
        Ok(snapshots
            .get(namespace)
            .map(|ns_snapshots| ns_snapshots.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn delete_snapshot(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<()> {
        let mut snapshots = self.snapshots.write().await;
        if let Some(ns_snapshots) = snapshots.get_mut(namespace) {
            ns_snapshots.remove(snapshot_id);
        }
        Ok(())
    }

    async fn snapshot_metadata(
        &self,
        namespace: &StorageNamespace,
        snapshot_id: &str,
    ) -> StorageResult<SnapshotMetadata> {
        let snapshots = self.snapshots.read().await;
        let snapshot_data = snapshots
            .get(namespace)
            .and_then(|ns| ns.get(snapshot_id))
            .ok_or_else(|| {
                StorageError::SnapshotError(format!("Snapshot {} not found", snapshot_id))
            })?;

        Ok(SnapshotMetadata {
            id: snapshot_id.to_string(),
            size: snapshot_data.len() as u64,
            created_at: chrono::Utc::now().timestamp() as u64,
            namespace: namespace.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Test put and get
        let key = StorageKey::from_str("key1");
        let value = StorageValue::new(b"value1" as &[u8]);
        storage
            .put(&namespace, key.clone(), value.clone())
            .await
            .unwrap();

        let retrieved = storage.get(&namespace, &key).await.unwrap();
        assert_eq!(retrieved, Some(value));

        // Test delete
        storage.delete(&namespace, &key).await.unwrap();
        let retrieved = storage.get(&namespace, &key).await.unwrap();
        assert_eq!(retrieved, None);
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        let mut batch = WriteBatch::new();
        for i in 0..10 {
            batch.put(
                namespace.clone(),
                StorageKey::from_str(&format!("key{}", i)),
                StorageValue::new(format!("value{}", i).into_bytes()),
            );
        }

        storage.write_batch(batch).await.unwrap();

        // Verify all values were written
        for i in 0..10 {
            let key = StorageKey::from_str(&format!("key{}", i));
            let value = storage.get(&namespace, &key).await.unwrap();
            assert!(value.is_some());
        }
    }

    /* Commented out - LogStorage is now generic and domain-specific
    #[tokio::test]
    async fn test_log_operations() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("logs");

        // Append logs
        for i in 0..10 {
            storage
                .append_log(&namespace, i, Bytes::from(format!("log{}", i)))
                .await
                .unwrap();
        }

        // Read range
        let logs = storage.read_log_range(&namespace, 2, 5).await.unwrap();
        assert_eq!(logs.len(), 4); // indices 2, 3, 4, 5

        // Test truncate
        storage.truncate_log(&namespace, 5).await.unwrap();
        let last = storage.last_log_index(&namespace).await.unwrap();
        assert_eq!(last, Some(5));
    }
    */

    #[tokio::test]
    async fn test_snapshots() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Add some data
        for i in 0..5 {
            storage
                .put(
                    &namespace,
                    StorageKey::from_str(&format!("key{}", i)),
                    StorageValue::new(format!("value{}", i).into_bytes()),
                )
                .await
                .unwrap();
        }

        // Create snapshot
        let snapshot_data = storage
            .create_snapshot(&namespace, "snapshot1")
            .await
            .unwrap();

        // Clear the namespace
        storage.drop_namespace(&namespace).await.unwrap();
        storage.create_namespace(&namespace).await.unwrap();

        // Restore from snapshot
        storage
            .restore_snapshot(&namespace, "snapshot1", snapshot_data)
            .await
            .unwrap();

        // Verify data was restored
        for i in 0..5 {
            let key = StorageKey::from_str(&format!("key{}", i));
            let value = storage.get(&namespace, &key).await.unwrap();
            assert!(value.is_some());
        }
    }
}
