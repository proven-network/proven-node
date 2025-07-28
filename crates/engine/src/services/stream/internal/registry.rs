//! Stream registry for managing stream existence and storage instances

use std::sync::Arc;

use dashmap::DashMap;
use proven_storage::{StorageAdaptor, StorageManager};

use crate::foundation::{PersistenceType, StreamConfig, StreamName};

use super::storage::StreamStorageImpl;

/// Registry for managing stream existence and storage instances
///
/// This type encapsulates the logic for checking if a stream exists
/// and creating storage instances on demand, even for empty streams.
pub struct StreamRegistry<S>
where
    S: StorageAdaptor,
{
    /// Stream storage instances
    streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
    /// Stream configurations
    stream_configs: Arc<DashMap<StreamName, StreamConfig>>,
    /// Storage manager for creating persistent storage
    pub storage_manager: Arc<StorageManager<S>>,
}

impl<S> StreamRegistry<S>
where
    S: StorageAdaptor,
{
    /// Create a new stream registry
    pub fn new(
        streams: Arc<DashMap<StreamName, Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>>>,
        stream_configs: Arc<DashMap<StreamName, StreamConfig>>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            streams,
            stream_configs,
            storage_manager,
        }
    }

    /// Check if a stream exists (either has config or storage)
    pub fn stream_exists(&self, stream_name: &StreamName) -> bool {
        self.stream_configs.contains_key(stream_name) || self.streams.contains_key(stream_name)
    }

    /// Get or create stream storage
    ///
    /// This will:
    /// 1. Return existing storage if available
    /// 2. Create storage for configured streams that have no data yet
    /// 3. Return None only if the stream truly doesn't exist
    pub async fn get_or_create_storage(
        &self,
        stream_name: &StreamName,
    ) -> Option<Arc<StreamStorageImpl<proven_storage::StreamStorage<S>>>> {
        // First check if we have it in memory
        if let Some(storage) = self.streams.get(stream_name) {
            return Some(storage.clone());
        }

        // Check if the stream exists in config (meaning it was created but has no data)
        if let Some(config) = self.stream_configs.get(stream_name) {
            // Create storage based on persistence type
            let storage = match config.persistence_type {
                PersistenceType::Persistent => {
                    let namespace =
                        proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));
                    Arc::new(StreamStorageImpl::persistent(
                        stream_name.clone(),
                        self.storage_manager.stream_storage(),
                        namespace,
                    ))
                }
                PersistenceType::Ephemeral => {
                    Arc::new(StreamStorageImpl::ephemeral(stream_name.clone()))
                }
            };

            // Store it for future use
            self.streams.insert(stream_name.clone(), storage.clone());

            return Some(storage);
        }

        // Stream doesn't exist at all
        None
    }

    /// Get stream configuration
    pub fn get_stream_config(&self, stream_name: &StreamName) -> Option<StreamConfig> {
        self.stream_configs
            .get(stream_name)
            .map(|entry| entry.value().clone())
    }
}

impl<S> Clone for StreamRegistry<S>
where
    S: StorageAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            streams: self.streams.clone(),
            stream_configs: self.stream_configs.clone(),
            storage_manager: self.storage_manager.clone(),
        }
    }
}
