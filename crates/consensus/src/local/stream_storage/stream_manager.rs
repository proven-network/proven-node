//! Stream manager for handling per-stream storage instances
//!
//! This module manages individual storage instances for each stream,
//! enabling migration and per-stream configuration.

use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::local::stream_storage::per_stream_factory::{
    PerStreamStorageFactory, StreamPersistenceMode, StreamStorageBackend, UnifiedPerStreamFactory,
};
use crate::local::stream_storage::traits::{StorageType, StreamConfig};
use crate::storage::generic::GenericStorage;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for the unified stream manager
pub type UnifiedStreamManager = StreamManager;

/// Create a new unified stream manager
pub fn create_stream_manager(
    group_id: ConsensusGroupId,
    base_path: Option<PathBuf>,
) -> ConsensusResult<UnifiedStreamManager> {
    create_stream_manager_with_backend(group_id, base_path, StreamStorageBackend::default())
}

/// Create a new unified stream manager with a specific storage backend
pub fn create_stream_manager_with_backend(
    group_id: ConsensusGroupId,
    base_path: Option<PathBuf>,
    storage_backend: StreamStorageBackend,
) -> ConsensusResult<UnifiedStreamManager> {
    let factory = Arc::new(UnifiedPerStreamFactory::with_backend(
        base_path,
        storage_backend,
    )?);
    Ok(StreamManager::new(group_id, factory))
}

/// Information about a managed stream
#[derive(Clone)]
struct ManagedStream {
    /// The storage instance for this stream
    storage: Arc<GenericStorage>,
    /// Stream configuration
    config: StreamConfig,
}

/// Manages storage instances for individual streams
pub struct StreamManager {
    /// Group ID this manager belongs to
    group_id: ConsensusGroupId,
    /// Map of stream ID to stream information
    streams: Arc<RwLock<HashMap<String, ManagedStream>>>,
    /// Factory for creating stream storage
    factory: Arc<UnifiedPerStreamFactory>,
}

impl StreamManager {
    /// Create a new stream manager
    pub fn new(group_id: ConsensusGroupId, factory: Arc<UnifiedPerStreamFactory>) -> Self {
        Self {
            group_id,
            streams: Arc::new(RwLock::new(HashMap::new())),
            factory,
        }
    }

    /// Create a new stream with its own storage instance
    pub async fn create_stream(
        &self,
        stream_id: String,
        config: StreamConfig,
    ) -> ConsensusResult<Arc<GenericStorage>> {
        let mut streams = self.streams.write().await;

        // Check if stream already exists
        if streams.contains_key(&stream_id) {
            return Err(Error::already_exists(format!(
                "Stream {} already exists",
                stream_id
            )));
        }

        // Convert storage type to persistence mode
        let persistence_mode = match config.storage_type {
            StorageType::Memory => StreamPersistenceMode::Memory,
            StorageType::File => StreamPersistenceMode::File,
        };

        // Create storage instance for this stream
        let storage = self
            .factory
            .create_stream_storage(self.group_id, &stream_id, &persistence_mode)
            .await?;

        // Store stream info
        let info = ManagedStream {
            storage: storage.clone(),
            config,
        };
        streams.insert(stream_id, info);

        Ok(storage)
    }

    /// Get the storage instance for a stream
    pub async fn get_stream_storage(
        &self,
        stream_id: &str,
    ) -> ConsensusResult<Arc<GenericStorage>> {
        let streams = self.streams.read().await;
        streams
            .get(stream_id)
            .map(|info| info.storage.clone())
            .ok_or_else(|| Error::not_found(format!("Stream {} not found", stream_id)))
    }

    /// Check if a stream exists
    pub async fn stream_exists(&self, stream_id: &str) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_id)
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_id: &str) -> ConsensusResult<StreamConfig> {
        let streams = self.streams.read().await;
        streams
            .get(stream_id)
            .map(|info| info.config.clone())
            .ok_or_else(|| Error::not_found(format!("Stream {} not found", stream_id)))
    }

    /// Update stream configuration
    pub async fn update_stream_config(
        &self,
        stream_id: &str,
        config: StreamConfig,
    ) -> ConsensusResult<()> {
        let mut streams = self.streams.write().await;
        match streams.get_mut(stream_id) {
            Some(info) => {
                info.config = config;
                Ok(())
            }
            None => Err(Error::not_found(format!("Stream {} not found", stream_id))),
        }
    }

    /// Remove a stream and clean up its storage
    pub async fn remove_stream(&self, stream_id: &str) -> ConsensusResult<()> {
        let mut streams = self.streams.write().await;

        // Remove from map
        if streams.remove(stream_id).is_none() {
            return Err(Error::not_found(format!("Stream {} not found", stream_id)));
        }

        // Clean up storage
        self.factory
            .cleanup_stream_storage(self.group_id, stream_id)?;

        Ok(())
    }

    /// List all managed streams
    pub async fn list_streams(&self) -> Vec<String> {
        let streams = self.streams.read().await;
        streams.keys().cloned().collect()
    }

    /// Get the number of managed streams
    pub async fn stream_count(&self) -> usize {
        let streams = self.streams.read().await;
        streams.len()
    }

    /// Export a stream for migration
    pub async fn export_stream(&self, stream_id: &str) -> ConsensusResult<StreamExport> {
        let streams = self.streams.read().await;
        let info = streams
            .get(stream_id)
            .ok_or_else(|| Error::not_found(format!("Stream {} not found", stream_id)))?;

        Ok(StreamExport {
            stream_id: stream_id.to_string(),
            config: info.config.clone(),
            storage: info.storage.clone(),
            group_id: self.group_id,
        })
    }

    /// Import a stream from another group (used during migration)
    pub async fn import_stream(
        &self,
        stream_id: String,
        config: StreamConfig,
        _source_storage: Arc<GenericStorage>,
    ) -> ConsensusResult<Arc<GenericStorage>> {
        let mut streams = self.streams.write().await;

        // Check if stream already exists
        if streams.contains_key(&stream_id) {
            return Err(Error::already_exists(format!(
                "Stream {} already exists",
                stream_id
            )));
        }

        // Convert storage type to persistence mode
        let persistence_mode = match config.storage_type {
            StorageType::Memory => StreamPersistenceMode::Memory,
            StorageType::File => StreamPersistenceMode::File,
        };

        // Create new storage instance for this stream in our group
        let new_storage = self
            .factory
            .create_stream_storage(self.group_id, &stream_id, &persistence_mode)
            .await?;

        // TODO: Copy data from source_storage to new_storage
        // This would involve iterating through all namespaces and copying data

        // Store stream info
        let info = ManagedStream {
            storage: new_storage.clone(),
            config,
        };
        streams.insert(stream_id, info);

        Ok(new_storage)
    }

    /// Clean up all streams (used when removing a group)
    pub async fn cleanup_all(&self) -> ConsensusResult<()> {
        let stream_ids: Vec<String> = {
            let streams = self.streams.read().await;
            streams.keys().cloned().collect()
        };

        // Remove each stream
        for stream_id in stream_ids {
            self.remove_stream(&stream_id).await?;
        }

        Ok(())
    }
}

/// Stream export data for migration
pub struct StreamExport {
    /// Stream identifier
    pub stream_id: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Storage instance
    pub storage: Arc<GenericStorage>,
    /// Source group ID
    pub group_id: ConsensusGroupId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::stream_storage::traits::{CompressionType, RetentionPolicy, StorageType};

    #[tokio::test]
    async fn test_create_stream_with_memory_storage() {
        let group_id = ConsensusGroupId(1);
        let manager = create_stream_manager(group_id, None).unwrap();

        let config = StreamConfig {
            max_messages: Some(1000),
            max_bytes: None,
            max_age_secs: Some(3600),
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: None,
            compact_on_deletion: false,
            compression: CompressionType::None,
        };

        let storage = manager
            .create_stream("test_stream".to_string(), config)
            .await
            .unwrap();

        assert_eq!(storage.storage_type(), "memory");
        assert!(manager.stream_exists("test_stream").await);
    }

    #[tokio::test]
    async fn test_create_stream_with_rocksdb_storage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let group_id = ConsensusGroupId(1);
        let manager = create_stream_manager(group_id, Some(temp_dir.path().to_path_buf())).unwrap();

        let config = StreamConfig {
            max_messages: None,
            max_bytes: None,
            max_age_secs: None,
            storage_type: StorageType::File,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: None,
            compact_on_deletion: true,
            compression: CompressionType::Lz4,
        };

        let storage = manager
            .create_stream("test_stream".to_string(), config)
            .await
            .unwrap();

        assert_eq!(storage.storage_type(), "rocksdb");
        assert!(manager.stream_exists("test_stream").await);

        // Cleanup
        manager.remove_stream("test_stream").await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_isolation() {
        let group_id = ConsensusGroupId(1);
        let manager = create_stream_manager(group_id, None).unwrap();

        let config = StreamConfig {
            max_messages: Some(1000),
            max_bytes: None,
            max_age_secs: Some(3600),
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: None,
            compact_on_deletion: false,
            compression: CompressionType::None,
        };

        // Create two streams
        let storage1 = manager
            .create_stream("stream1".to_string(), config.clone())
            .await
            .unwrap();

        let storage2 = manager
            .create_stream("stream2".to_string(), config)
            .await
            .unwrap();

        // Verify they are different instances
        assert!(!Arc::ptr_eq(&storage1, &storage2));

        // Verify both exist
        assert!(manager.stream_exists("stream1").await);
        assert!(manager.stream_exists("stream2").await);

        // Remove one stream
        manager.remove_stream("stream1").await.unwrap();

        // Verify only one remains
        assert!(!manager.stream_exists("stream1").await);
        assert!(manager.stream_exists("stream2").await);
    }
}
