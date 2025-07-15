//! In-memory log storage implementation

use async_trait::async_trait;
use bytes::Bytes;
use proven_storage::{StorageNamespace, StorageResult};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;

/// In-memory log storage implementation using BTreeMap for ordering
#[derive(Clone)]
pub struct MemoryStorage {
    /// Log storage: namespace -> (index -> bytes)
    logs: Arc<RwLock<HashMap<StorageNamespace, BTreeMap<u64, Bytes>>>>,
    /// Log bounds cache: namespace -> (first_index, last_index)
    log_bounds: Arc<RwLock<HashMap<StorageNamespace, (u64, u64)>>>,
}

impl MemoryStorage {
    /// Create a new in-memory storage instance
    pub fn new() -> Self {
        Self {
            logs: Arc::new(RwLock::new(HashMap::new())),
            log_bounds: Arc::new(RwLock::new(HashMap::new())),
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
        entries: Vec<(u64, Bytes)>,
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        let btree = logs.entry(namespace.clone()).or_insert_with(BTreeMap::new);

        // Get current bounds or initialize
        let (mut first_index, mut last_index) =
            bounds.get(namespace).copied().unwrap_or_else(|| {
                if let Some((&first, _)) = btree.iter().next() {
                    let last = btree.iter().next_back().map(|(&k, _)| k).unwrap_or(first);
                    (first, last)
                } else {
                    (u64::MAX, 0)
                }
            });

        // Insert all entries
        for (index, data) in entries {
            btree.insert(index, data);

            // Update bounds
            if first_index == u64::MAX || index < first_index {
                first_index = index;
            }
            if index > last_index {
                last_index = index;
            }
        }

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

    async fn truncate_after(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        if let Some(btree) = logs.get_mut(namespace) {
            // Collect indices to remove
            let to_remove: Vec<_> = btree.range((index + 1)..).map(|(&idx, _)| idx).collect();

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

    async fn compact_before(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        let mut logs = self.logs.write().await;
        let mut bounds = self.log_bounds.write().await;

        if let Some(btree) = logs.get_mut(namespace) {
            // Collect indices to remove
            let to_remove: Vec<_> = btree.range(..=index).map(|(&idx, _)| idx).collect();

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

    async fn bounds(&self, namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>> {
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
}

impl std::fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("logs", &"<locked>")
            .field("log_bounds", &"<locked>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_storage::LogStorage;

    #[tokio::test]
    async fn test_log_storage_append_and_get() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Test single entry append
        let entries = vec![(1, Bytes::from("test data 1"))];
        storage.append(&namespace, entries).await.unwrap();

        // Test read single entry via range
        let result = storage.read_range(&namespace, 1, 2).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (1, Bytes::from("test data 1")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((1, 1)));
    }

    #[tokio::test]
    async fn test_log_storage_multiple_append() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Append multiple entries
        let entries = vec![
            (1, Bytes::from("data 1")),
            (2, Bytes::from("data 2")),
            (3, Bytes::from("data 3")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        // Test read range
        let range = storage.read_range(&namespace, 1, 4).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (1, Bytes::from("data 1")));
        assert_eq!(range[1], (2, Bytes::from("data 2")));
        assert_eq!(range[2], (3, Bytes::from("data 3")));

        // Test bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((1, 3)));
    }

    #[tokio::test]
    async fn test_log_storage_truncate() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Append entries
        let entries = vec![
            (1, Bytes::from("data 1")),
            (2, Bytes::from("data 2")),
            (3, Bytes::from("data 3")),
            (4, Bytes::from("data 4")),
            (5, Bytes::from("data 5")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        // Truncate after index 3
        storage.truncate_after(&namespace, 3).await.unwrap();

        // Check remaining entries
        let range = storage.read_range(&namespace, 1, 6).await.unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, 1);
        assert_eq!(range[1].0, 2);
        assert_eq!(range[2].0, 3);

        // Check bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((1, 3)));
    }

    #[tokio::test]
    async fn test_log_storage_compact() {
        let storage = MemoryStorage::new();
        let namespace = StorageNamespace::new("test");

        // Append entries
        let entries = vec![
            (1, Bytes::from("data 1")),
            (2, Bytes::from("data 2")),
            (3, Bytes::from("data 3")),
            (4, Bytes::from("data 4")),
            (5, Bytes::from("data 5")),
        ];
        storage.append(&namespace, entries).await.unwrap();

        // Compact before index 3
        storage.compact_before(&namespace, 3).await.unwrap();

        // Check remaining entries
        let range = storage.read_range(&namespace, 1, 6).await.unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, 4);
        assert_eq!(range[1].0, 5);

        // Check bounds
        let bounds = storage.bounds(&namespace).await.unwrap();
        assert_eq!(bounds, Some((4, 5)));
    }
}
