//! In-memory log storage implementation

use async_trait::async_trait;
use bytes::Bytes;
use proven_storage::{StorageAdaptor, StorageNamespace, StorageResult};
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZero,
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio_stream::Stream;

/// Type aliases to reduce type complexity
type LogStorage = Arc<RwLock<HashMap<StorageNamespace, BTreeMap<NonZero<u64>, Bytes>>>>;
type LogBoundsCache = Arc<RwLock<HashMap<StorageNamespace, (NonZero<u64>, NonZero<u64>)>>>;
type MetadataStorage = Arc<RwLock<HashMap<StorageNamespace, HashMap<String, Bytes>>>>;

/// In-memory log storage implementation using BTreeMap for ordering
#[derive(Clone)]
pub struct MemoryStorage {
    /// Log storage: namespace -> (index -> bytes)
    logs: LogStorage,
    /// Log bounds cache: namespace -> (first_index, last_index)
    log_bounds: LogBoundsCache,
    /// Metadata storage: namespace -> (key -> value)
    metadata: MetadataStorage,
}

impl MemoryStorage {
    /// Create a new in-memory storage instance
    pub fn new() -> Self {
        Self {
            logs: Arc::new(RwLock::new(HashMap::new())),
            log_bounds: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

// Simplified LogStorage implementation for in-memory storage
#[async_trait]
impl proven_storage::LogStorage for MemoryStorage {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Arc<Vec<Bytes>>,
    ) -> StorageResult<NonZero<u64>> {
        if entries.is_empty() {
            return Err(proven_storage::StorageError::InvalidValue(
                "Cannot append empty entries".to_string(),
            ));
        }

        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        let btree = logs.entry(namespace.clone()).or_insert_with(BTreeMap::new);

        // Get the next index based on current bounds
        let start_index = if let Some((_, last)) = bounds.get(namespace) {
            NonZero::new(last.get() + 1).unwrap()
        } else {
            NonZero::new(1).unwrap()
        };

        // Insert all entries sequentially
        let mut last_index = start_index;
        for (i, data) in entries.iter().enumerate() {
            let index = NonZero::new(start_index.get() + i as u64).unwrap();
            btree.insert(index, data.clone());
            last_index = index;
        }

        // Update bounds cache
        let (first, _) = bounds
            .get(namespace)
            .copied()
            .unwrap_or((start_index, last_index));
        bounds.insert(namespace.clone(), (first, last_index));

        Ok(last_index)
    }

    async fn put_at(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(NonZero<u64>, Arc<Bytes>)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        let btree = logs.entry(namespace.clone()).or_insert_with(BTreeMap::new);

        // Get current bounds or initialize
        let existing_bounds = bounds.get(namespace).copied();

        // Insert all entries and track bounds
        let mut first_index: Option<NonZero<u64>> = existing_bounds.map(|(first, _)| first);
        let mut last_index: Option<NonZero<u64>> = existing_bounds.map(|(_, last)| last);

        for (index, data) in entries {
            btree.insert(index, (*data).clone());

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

        // Update bounds cache if we have any entries
        if let (Some(first), Some(last)) = (first_index, last_index) {
            bounds.insert(namespace.clone(), (first, last));
        }

        Ok(())
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: NonZero<u64>,
        end: NonZero<u64>,
    ) -> StorageResult<Vec<(NonZero<u64>, Bytes)>> {
        let logs = self.logs.read().await;

        if let Some(btree) = logs.get(namespace) {
            let entries: Vec<_> = btree
                .range(start..end)
                .map(|(&idx, data)| (idx, data.clone()))
                .collect();
            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        if let Some(btree) = logs.get_mut(namespace) {
            // Create next index for the range
            let next_index = NonZero::new(index.get() + 1);

            // Collect indices to remove
            let to_remove: Vec<_> = if let Some(next) = next_index {
                btree.range(next..).map(|(&idx, _)| idx).collect()
            } else {
                // If index + 1 overflows, there's nothing to remove
                Vec::new()
            };

            // Remove entries
            for idx in to_remove {
                btree.remove(&idx);
            }

            // Update bounds
            if btree.is_empty() {
                bounds.remove(namespace);
            } else if let Some((_first, last)) = bounds.get_mut(namespace)
                && let Some((&new_last, _)) = btree.iter().next_back()
            {
                *last = new_last;
            }
        }

        Ok(())
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        if let Some(btree) = logs.get_mut(namespace) {
            // Collect indices to remove (up to and including index)
            // We need to use Included bound for NonZero<u64>
            use std::ops::Bound;
            let to_remove: Vec<_> = btree
                .range((Bound::Unbounded, Bound::Included(index)))
                .map(|(&idx, _)| idx)
                .collect();

            // Remove entries
            for idx in to_remove {
                btree.remove(&idx);
            }

            // Update bounds
            if btree.is_empty() {
                bounds.remove(namespace);
            } else if let Some((first, _last)) = bounds.get_mut(namespace)
                && let Some((&new_first, _)) = btree.iter().next()
            {
                *first = new_first;
            }
        }

        Ok(())
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(NonZero<u64>, NonZero<u64>)>> {
        // First check cache
        {
            let bounds = self.log_bounds.read().await;
            if let Some(&cached) = bounds.get(namespace) {
                return Ok(Some(cached));
            }
        }

        // If not in cache, compute from data
        let logs = self.logs.read().await;
        if let Some(btree) = logs.get(namespace) {
            if btree.is_empty() {
                Ok(None)
            } else {
                let first = btree.iter().next().map(|(&k, _)| k).unwrap();
                let last = btree.iter().next_back().map(|(&k, _)| k).unwrap();

                // Update cache
                drop(logs);
                let mut bounds = self.log_bounds.write().await;
                bounds.insert(namespace.clone(), (first, last));

                Ok(Some((first, last)))
            }
        } else {
            Ok(None)
        }
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        let metadata = self.metadata.read().await;
        if let Some(namespace_metadata) = metadata.get(namespace) {
            Ok(namespace_metadata.get(key).cloned())
        } else {
            Ok(None)
        }
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        let mut metadata = self.metadata.write().await;
        let namespace_metadata = metadata
            .entry(namespace.clone())
            .or_insert_with(HashMap::new);
        namespace_metadata.insert(key.to_string(), value);
        Ok(())
    }
}

// Implement StorageAdaptor for MemoryStorage
// Since MemoryStorage already implements LogStorage + LogStorageWithDelete,
// we just need to implement the trait with any additional methods
impl StorageAdaptor for MemoryStorage {
    // Use default implementations
    // shutdown() uses default implementation since memory storage doesn't need cleanup
}

// Implement LogStorageWithDelete for MemoryStorage
#[async_trait]
impl proven_storage::LogStorageWithDelete for MemoryStorage {
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<bool> {
        let mut logs = self.logs.write().await;

        if let Some(btree) = logs.get_mut(namespace) {
            // Check if the entry exists and remove it
            if btree.remove(&index).is_some() {
                // Entry was deleted
                // Note: We don't update bounds here because deletion of a single entry
                // in the middle doesn't change the first/last bounds
                Ok(true)
            } else {
                // Entry didn't exist
                Ok(false)
            }
        } else {
            // Namespace doesn't exist
            Ok(false)
        }
    }
}

// Implement LogStorageStreaming for MemoryStorage
#[async_trait]
impl proven_storage::LogStorageStreaming for MemoryStorage {
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: NonZero<u64>,
        end: Option<NonZero<u64>>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(NonZero<u64>, Bytes)>> + Send + Unpin>>
    {
        // Clone the data we need to stream
        let logs_guard = self.logs.read().await;
        let entries: Vec<(NonZero<u64>, Bytes)> = if let Some(btree) = logs_guard.get(namespace) {
            match end {
                Some(end_idx) => btree
                    .range(start..end_idx)
                    .map(|(&idx, data)| (idx, data.clone()))
                    .collect(),
                None => btree
                    .range(start..)
                    .map(|(&idx, data)| (idx, data.clone()))
                    .collect(),
            }
        } else {
            Vec::new()
        };
        drop(logs_guard);

        // Create a stream from the collected entries
        let stream = tokio_stream::iter(entries.into_iter().map(Ok));

        Ok(Box::new(stream))
    }
}

impl std::fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("logs", &"<locked>")
            .field("log_bounds", &"<locked>")
            .field("metadata", &"<locked>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_storage::{LogStorage, LogStorageStreaming, LogStorageWithDelete};
    use tokio_stream::StreamExt;

    // Helper function to create NonZero<u64>
    fn nz(n: u64) -> NonZero<u64> {
        NonZero::new(n).expect("test indices should be non-zero")
    }

    #[tokio::test]
    async fn test_log_storage_append_and_get() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Test single entry append
        let entries = Arc::new(vec![Bytes::from("test data 1")]);
        let last_seq = storage.append(&namespace, entries).await.unwrap();
        assert_eq!(last_seq, nz(1));

        // Test read single entry via range
        let result = storage.read_range(&namespace, nz(1), nz(2)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (nz(1), Bytes::from("test data 1")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(1))));
    }

    #[tokio::test]
    async fn test_log_storage_multiple_append() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Append multiple entries
        let entries = Arc::new(vec![
            Bytes::from("data 1"),
            Bytes::from("data 2"),
            Bytes::from("data 3"),
        ]);
        let last_seq = storage.append(&namespace, entries).await.unwrap();
        assert_eq!(last_seq, nz(3));

        // Test read range
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
    async fn test_log_storage_truncate() {
        let storage = MemoryStorage::new();
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

        // Truncate after index 3
        storage.truncate_after(&namespace, nz(3)).await.unwrap();

        // Check remaining entries
        let range = storage.read_range(&namespace, nz(1), nz(6)).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, nz(1));
        assert_eq!(range[1].0, nz(2));
        assert_eq!(range[2].0, nz(3));

        // Check bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(3))));
    }

    #[tokio::test]
    async fn test_log_storage_compact() {
        let storage = MemoryStorage::new();
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

        // Compact before index 3
        storage.compact_before(&namespace, nz(3)).await.unwrap();

        // Check remaining entries
        let range = storage.read_range(&namespace, nz(1), nz(6)).await.unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, nz(4));
        assert_eq!(range[1].0, nz(5));

        // Check bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(4), nz(5))));
    }

    #[tokio::test]
    async fn test_log_storage_delete_entry() {
        let storage = MemoryStorage::new();
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

        // Delete entry at index 3
        let deleted = storage.delete_entry(&namespace, nz(3)).await.unwrap();
        assert!(deleted);

        // Try to delete the same entry again - should return false
        let deleted_again = storage.delete_entry(&namespace, nz(3)).await.unwrap();
        assert!(!deleted_again);

        // Check remaining entries
        let range = storage.read_range(&namespace, nz(1), nz(6)).await.unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0].0, nz(1));
        assert_eq!(range[1].0, nz(2));
        assert_eq!(range[2].0, nz(4));
        assert_eq!(range[3].0, nz(5));

        // Check bounds - should remain unchanged
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(5))));

        // Delete non-existent entry
        let deleted_nonexistent = storage.delete_entry(&namespace, nz(10)).await.unwrap();
        assert!(!deleted_nonexistent);
    }

    #[tokio::test]
    async fn test_log_storage_streaming() {
        let storage = MemoryStorage::new();
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

    #[tokio::test]
    async fn test_random_access_storage() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Put entries at specific indices (non-sequential)
        let entries = vec![
            (nz(5), Arc::new(Bytes::from("data 5"))),
            (nz(2), Arc::new(Bytes::from("data 2"))),
            (nz(8), Arc::new(Bytes::from("data 8"))),
            (nz(1), Arc::new(Bytes::from("data 1"))),
        ];
        storage.put_at(&namespace, entries).await.unwrap();

        // Check bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((nz(1), nz(8))));

        // Read the entries
        let range = storage.read_range(&namespace, nz(1), nz(9)).await.unwrap();
        assert_eq!(range.len(), 4);
        assert_eq!(range[0], (nz(1), Bytes::from("data 1")));
        assert_eq!(range[1], (nz(2), Bytes::from("data 2")));
        assert_eq!(range[2], (nz(5), Bytes::from("data 5")));
        assert_eq!(range[3], (nz(8), Bytes::from("data 8")));

        // Overwrite an entry
        let overwrite = vec![(nz(5), Arc::new(Bytes::from("updated data 5")))];
        storage.put_at(&namespace, overwrite).await.unwrap();

        // Verify overwrite
        let range = storage.read_range(&namespace, nz(5), nz(6)).await.unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0], (nz(5), Bytes::from("updated data 5")));
    }
}
