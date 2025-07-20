//! Storage manager for coordinating storage operations
//!
//! This module provides a concrete StorageManager type that wraps storage adaptors
//! and provides specialized storage interfaces for different use cases.

use async_trait::async_trait;
use bytes::Bytes;
use std::{num::NonZero, sync::Arc};
use tokio_stream::Stream;
use tracing::{debug, info};

use crate::{
    LogStorage, LogStorageStreaming, LogStorageWithDelete, StorageNamespace, StorageResult,
    adaptor::StorageAdaptor,
};

/// Storage view for consensus operations (read-only logs)
#[derive(Clone)]
pub struct ConsensusStorage<S> {
    adaptor: Arc<S>,
    namespace_prefix: String,
}

/// Storage view for stream operations (deletable logs)
#[derive(Clone)]
pub struct StreamStorage<S> {
    adaptor: Arc<S>,
    namespace_prefix: String,
}

impl<S> ConsensusStorage<S> {
    /// Create a prefixed namespace for consensus storage
    fn prefixed_namespace(&self, namespace: &StorageNamespace) -> StorageNamespace {
        StorageNamespace::new(format!("{}:{}", self.namespace_prefix, namespace))
    }
}

impl<S> StreamStorage<S> {
    /// Create a prefixed namespace for stream storage
    fn prefixed_namespace(&self, namespace: &StorageNamespace) -> StorageNamespace {
        StorageNamespace::new(format!("{}:{}", self.namespace_prefix, namespace))
    }
}

/// Manages storage operations through a storage adaptor
///
/// This type provides specialized storage interfaces for different use cases,
/// ensuring proper separation of concerns and automatic namespacing.
#[derive(Clone)]
pub struct StorageManager<S>
where
    S: StorageAdaptor,
{
    /// The storage adaptor
    adaptor: Arc<S>,
}

impl<S> StorageManager<S>
where
    S: StorageAdaptor,
{
    /// Create a new storage manager with the given adaptor
    pub fn new(adaptor: S) -> Self {
        info!("Creating storage manager");

        Self {
            adaptor: Arc::new(adaptor),
        }
    }

    /// Create a storage manager from an already Arc-wrapped adaptor
    pub fn from_arc(adaptor: Arc<S>) -> Self {
        info!("Creating storage manager from Arc");

        Self { adaptor }
    }

    /// Get storage for consensus operations (no delete capability)
    ///
    /// This returns a storage view that only implements LogStorage,
    /// preventing accidental deletion of consensus entries.
    pub fn consensus_storage(&self) -> ConsensusStorage<S> {
        ConsensusStorage {
            adaptor: self.adaptor.clone(),
            namespace_prefix: "consensus".to_string(),
        }
    }

    /// Get storage for stream operations (with delete capability)
    ///
    /// This returns a storage view that implements LogStorageWithDelete,
    /// allowing deletion of stream entries.
    pub fn stream_storage(&self) -> StreamStorage<S> {
        StreamStorage {
            adaptor: self.adaptor.clone(),
            namespace_prefix: "stream".to_string(),
        }
    }

    /// Shutdown the storage manager
    pub async fn shutdown(&self) -> StorageResult<()> {
        info!("Shutting down storage manager");
        self.adaptor.shutdown().await?;
        info!("Storage manager shut down");
        Ok(())
    }
}

impl<S: StorageAdaptor> std::fmt::Debug for StorageManager<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageManager")
            .field("adaptor_type", &std::any::type_name::<S>())
            .finish()
    }
}

/// Implement LogStorage for ConsensusStorage (no delete capability)
#[async_trait]
impl<S: StorageAdaptor> LogStorage for ConsensusStorage<S> {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(NonZero<u64>, Bytes)>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: appending {} entries to namespace {}",
            entries.len(),
            prefixed
        );
        LogStorage::append(&*self.adaptor, &prefixed, entries).await
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(NonZero<u64>, NonZero<u64>)>> {
        let prefixed = self.prefixed_namespace(namespace);
        LogStorage::bounds(&*self.adaptor, &prefixed).await
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: compacting namespace {} before index {}",
            prefixed, index
        );
        LogStorage::compact_before(&*self.adaptor, &prefixed, index).await
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: NonZero<u64>,
        end: NonZero<u64>,
    ) -> StorageResult<Vec<(NonZero<u64>, Bytes)>> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: reading range [{}, {}) from namespace {}",
            start, end, prefixed
        );
        LogStorage::read_range(&*self.adaptor, &prefixed, start, end).await
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: truncating namespace {} after index {}",
            prefixed, index
        );
        LogStorage::truncate_after(&*self.adaptor, &prefixed, index).await
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: getting metadata key {} from namespace {}",
            key, prefixed
        );
        LogStorage::get_metadata(&*self.adaptor, &prefixed, key).await
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "ConsensusStorage: setting metadata key {} in namespace {}",
            key, prefixed
        );
        LogStorage::set_metadata(&*self.adaptor, &prefixed, key, value).await
    }
}

impl<S: StorageAdaptor> std::fmt::Debug for ConsensusStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusStorage")
            .field("namespace_prefix", &self.namespace_prefix)
            .field("adaptor_type", &std::any::type_name::<S>())
            .finish()
    }
}

/// Implement LogStorage for StreamStorage
#[async_trait]
impl<S: StorageAdaptor> LogStorage for StreamStorage<S> {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(NonZero<u64>, Bytes)>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: appending {} entries to namespace {}",
            entries.len(),
            prefixed
        );
        LogStorage::append(&*self.adaptor, &prefixed, entries).await
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(NonZero<u64>, NonZero<u64>)>> {
        let prefixed = self.prefixed_namespace(namespace);
        LogStorage::bounds(&*self.adaptor, &prefixed).await
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: compacting namespace {} before index {}",
            prefixed, index
        );
        LogStorage::compact_before(&*self.adaptor, &prefixed, index).await
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: NonZero<u64>,
        end: NonZero<u64>,
    ) -> StorageResult<Vec<(NonZero<u64>, Bytes)>> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: reading range [{}, {}) from namespace {}",
            start, end, prefixed
        );
        LogStorage::read_range(&*self.adaptor, &prefixed, start, end).await
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: truncating namespace {} after index {}",
            prefixed, index
        );
        LogStorage::truncate_after(&*self.adaptor, &prefixed, index).await
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: getting metadata key {} from namespace {}",
            key, prefixed
        );
        LogStorage::get_metadata(&*self.adaptor, &prefixed, key).await
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: setting metadata key {} in namespace {}",
            key, prefixed
        );
        LogStorage::set_metadata(&*self.adaptor, &prefixed, key, value).await
    }
}

/// Implement LogStorageWithDelete for StreamStorage (allows deletion)
#[async_trait]
impl<S: StorageAdaptor> LogStorageWithDelete for StreamStorage<S> {
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: NonZero<u64>,
    ) -> StorageResult<bool> {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: deleting entry at index {} in namespace {}",
            index, prefixed
        );
        LogStorageWithDelete::delete_entry(&*self.adaptor, &prefixed, index).await
    }
}

/// Implement LogStorageStreaming for StreamStorage (if adaptor supports it)
#[async_trait]
impl<S> LogStorageStreaming for StreamStorage<S>
where
    S: StorageAdaptor + LogStorageStreaming,
{
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: NonZero<u64>,
        end: Option<NonZero<u64>>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(NonZero<u64>, Bytes)>> + Send + Unpin>>
    {
        let prefixed = self.prefixed_namespace(namespace);
        debug!(
            "StreamStorage: streaming range from {} in namespace {}",
            start, prefixed
        );
        LogStorageStreaming::stream_range(&*self.adaptor, &prefixed, start, end).await
    }
}

impl<S: StorageAdaptor> std::fmt::Debug for StreamStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamStorage")
            .field("namespace_prefix", &self.namespace_prefix)
            .field("adaptor_type", &std::any::type_name::<S>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use tokio::sync::RwLock;

    /// Mock storage adaptor for testing
    #[derive(Clone, Debug)]
    struct MockAdaptor {
        #[allow(clippy::type_complexity)]
        data: Arc<RwLock<HashMap<String, Vec<(NonZero<u64>, Bytes)>>>>,
    }

    impl MockAdaptor {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl LogStorage for MockAdaptor {
        async fn append(
            &self,
            namespace: &StorageNamespace,
            entries: Vec<(NonZero<u64>, Bytes)>,
        ) -> StorageResult<()> {
            let mut data = self.data.write().await;
            let ns_entries = data.entry(namespace.as_str().to_string()).or_default();
            ns_entries.extend(entries);
            Ok(())
        }

        async fn bounds(
            &self,
            namespace: &StorageNamespace,
        ) -> StorageResult<Option<(NonZero<u64>, NonZero<u64>)>> {
            let data = self.data.read().await;
            if let Some(entries) = data.get(namespace.as_str()) {
                if entries.is_empty() {
                    Ok(None)
                } else {
                    let first = entries.iter().map(|(idx, _)| *idx).min().unwrap();
                    let last = entries.iter().map(|(idx, _)| *idx).max().unwrap();
                    Ok(Some((first, last)))
                }
            } else {
                Ok(None)
            }
        }

        async fn compact_before(
            &self,
            _namespace: &StorageNamespace,
            _index: NonZero<u64>,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn read_range(
            &self,
            namespace: &StorageNamespace,
            start: NonZero<u64>,
            end: NonZero<u64>,
        ) -> StorageResult<Vec<(NonZero<u64>, Bytes)>> {
            let data = self.data.read().await;
            if let Some(entries) = data.get(namespace.as_str()) {
                Ok(entries
                    .iter()
                    .filter(|(idx, _)| *idx >= start && *idx < end)
                    .cloned()
                    .collect())
            } else {
                Ok(vec![])
            }
        }

        async fn truncate_after(
            &self,
            _namespace: &StorageNamespace,
            _index: NonZero<u64>,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn get_metadata(
            &self,
            _namespace: &StorageNamespace,
            _key: &str,
        ) -> StorageResult<Option<Bytes>> {
            Ok(None)
        }

        async fn set_metadata(
            &self,
            _namespace: &StorageNamespace,
            _key: &str,
            _value: Bytes,
        ) -> StorageResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl LogStorageWithDelete for MockAdaptor {
        async fn delete_entry(
            &self,
            namespace: &StorageNamespace,
            index: NonZero<u64>,
        ) -> StorageResult<bool> {
            let mut data = self.data.write().await;
            if let Some(entries) = data.get_mut(namespace.as_str()) {
                let original_len = entries.len();
                entries.retain(|(idx, _)| *idx != index);
                Ok(entries.len() < original_len)
            } else {
                Ok(false)
            }
        }
    }

    #[async_trait]
    impl LogStorageStreaming for MockAdaptor {
        async fn stream_range(
            &self,
            namespace: &StorageNamespace,
            start: NonZero<u64>,
            end: Option<NonZero<u64>>,
        ) -> StorageResult<
            Box<dyn Stream<Item = StorageResult<(NonZero<u64>, Bytes)>> + Send + Unpin>,
        > {
            let data = self.data.read().await;
            if let Some(entries) = data.get(namespace.as_str()) {
                // Filter entries based on the range [start, end)
                let mut filtered: Vec<_> = entries
                    .iter()
                    .filter(|(idx, _)| *idx >= start && (end.is_none() || *idx < end.unwrap()))
                    .map(|(idx, data)| (*idx, data.clone()))
                    .collect();

                // Sort by index to ensure entries are in order
                filtered.sort_by_key(|(idx, _)| *idx);

                Ok(Box::new(futures::stream::iter(
                    filtered.into_iter().map(Ok),
                )))
            } else {
                Ok(Box::new(futures::stream::empty()))
            }
        }
    }

    impl StorageAdaptor for MockAdaptor {
        // Use default implementations
    }

    #[tokio::test]
    async fn test_discrete_storage_views_with_automatic_namespacing() {
        let adaptor = MockAdaptor::new();
        let manager = StorageManager::new(adaptor.clone());

        // Get discrete storage views
        let consensus = manager.consensus_storage();
        let stream = manager.stream_storage();

        // Use the same namespace for both views
        let namespace = StorageNamespace::new("myapp");

        // Append to consensus storage
        consensus
            .append(
                &namespace,
                vec![(NonZero::new(1).unwrap(), Bytes::from("consensus data"))],
            )
            .await
            .unwrap();

        // Append to stream storage
        stream
            .append(
                &namespace,
                vec![(NonZero::new(1).unwrap(), Bytes::from("stream data"))],
            )
            .await
            .unwrap();

        // Verify they're stored in different namespaces
        let data = adaptor.data.read().await;
        assert!(data.contains_key("consensus:myapp"));
        assert!(data.contains_key("stream:myapp"));

        // Verify data isolation
        let consensus_data = &data["consensus:myapp"];
        assert_eq!(consensus_data.len(), 1);
        assert_eq!(consensus_data[0].1, Bytes::from("consensus data"));

        let stream_data = &data["stream:myapp"];
        assert_eq!(stream_data.len(), 1);
        assert_eq!(stream_data[0].1, Bytes::from("stream data"));
    }

    #[tokio::test]
    async fn test_consensus_storage_no_delete() {
        // This test verifies that ConsensusStorage implements LogStorage
        // and that it properly uses the consensus namespace prefix

        let adaptor = MockAdaptor::new();
        let manager = StorageManager::new(adaptor.clone());
        let consensus = manager.consensus_storage();

        let namespace = StorageNamespace::new("test");

        // Append some data using consensus storage
        consensus
            .append(
                &namespace,
                vec![(NonZero::new(1).unwrap(), Bytes::from("consensus entry"))],
            )
            .await
            .unwrap();

        // Verify it's stored with the consensus prefix
        let data = adaptor.data.read().await;
        assert!(data.contains_key("consensus:test"));
        assert!(!data.contains_key("test")); // Not stored without prefix

        // Note: ConsensusStorage intentionally does NOT implement LogStorageWithDelete
        // to prevent accidental deletion of consensus entries
    }

    #[tokio::test]
    async fn test_stream_storage_has_delete() {
        let adaptor = MockAdaptor::new();
        let manager = StorageManager::new(adaptor);
        let stream = manager.stream_storage();

        let namespace = StorageNamespace::new("test");

        // Append some data
        stream
            .append(
                &namespace,
                vec![
                    (NonZero::new(1).unwrap(), Bytes::from("data")),
                    (NonZero::new(2).unwrap(), Bytes::from("more data")),
                ],
            )
            .await
            .unwrap();

        // Delete entry - this works because StreamStorage implements LogStorageWithDelete
        let deleted = stream
            .delete_entry(&namespace, NonZero::new(1).unwrap())
            .await
            .unwrap();
        assert!(deleted);

        // Verify deletion
        let remaining = stream
            .read_range(
                &namespace,
                NonZero::new(1).unwrap(),
                NonZero::new(3).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, NonZero::new(2).unwrap());
    }

    #[tokio::test]
    async fn test_stream_storage_streaming() {
        use futures::StreamExt;

        let adaptor = MockAdaptor::new();
        let manager = StorageManager::new(adaptor);
        let stream = manager.stream_storage();

        let namespace = StorageNamespace::new("test");

        // Append data with various indices
        stream
            .append(
                &namespace,
                vec![
                    (NonZero::new(1).unwrap(), Bytes::from("one")),
                    (NonZero::new(3).unwrap(), Bytes::from("three")),
                    (NonZero::new(5).unwrap(), Bytes::from("five")),
                    (NonZero::new(7).unwrap(), Bytes::from("seven")),
                    (NonZero::new(9).unwrap(), Bytes::from("nine")),
                ],
            )
            .await
            .unwrap();

        // Test streaming with both start and end
        let stream_iter = stream
            .stream_range(
                &namespace,
                NonZero::new(3).unwrap(),
                Some(NonZero::new(8).unwrap()),
            )
            .await
            .unwrap();
        let results: Vec<_> = stream_iter.collect::<Vec<_>>().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().0, NonZero::new(3).unwrap());
        assert_eq!(results[1].as_ref().unwrap().0, NonZero::new(5).unwrap());
        assert_eq!(results[2].as_ref().unwrap().0, NonZero::new(7).unwrap());

        // Test streaming with no end (should stream to the end)
        let stream_iter = stream
            .stream_range(&namespace, NonZero::new(5).unwrap(), None)
            .await
            .unwrap();
        let results: Vec<_> = stream_iter.collect::<Vec<_>>().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap().0, NonZero::new(5).unwrap());
        assert_eq!(results[1].as_ref().unwrap().0, NonZero::new(7).unwrap());
        assert_eq!(results[2].as_ref().unwrap().0, NonZero::new(9).unwrap());
    }
}
