//! Stream service implementation
//!
//! This service manages stream storage and provides stream operations following
//! the established service patterns in the consensus engine.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, error, info, warn};

use proven_storage::{
    LogStorageWithDelete, StorageAdaptor, StorageManager, StorageNamespace, StreamStorage,
};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::{
    ConsensusGroupId,
    traits::{HealthStatus, ServiceHealth, ServiceLifecycle, SubsystemHealth},
};
use crate::services::event::{Event, EventEnvelope, EventFilter, EventService, EventType};
use crate::services::stream::config::RetentionPolicy;
use crate::services::stream::storage::{
    StreamStorageImpl, StreamStorageReader, StreamStorageWriter,
};
use crate::services::stream::{PersistenceType, StreamConfig, StreamName};

/// Type alias for stream storage map
type StreamStorageMap<S> = HashMap<StreamName, Arc<StreamStorageImpl<StreamStorage<S>>>>;

/// Stream metadata
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// Stream name
    pub name: StreamName,
    /// Stream configuration
    pub config: StreamConfig,
    /// Group ID that owns this stream
    pub group_id: crate::foundation::types::ConsensusGroupId,
    /// Creation timestamp
    pub created_at: u64,
    /// Last message timestamp
    pub last_message_at: Option<u64>,
    /// Total messages stored
    pub message_count: u64,
    /// Total bytes stored
    pub total_bytes: u64,
}

/// Stream service configuration
#[derive(Debug, Clone)]
pub struct StreamServiceConfig {
    /// Default persistence type for new streams
    pub default_persistence: PersistenceType,
    /// Whether to auto-create streams on first message
    pub auto_create: bool,
    /// Maximum number of streams to cache in memory
    pub max_cached_streams: usize,
}

impl Default for StreamServiceConfig {
    fn default() -> Self {
        Self {
            default_persistence: PersistenceType::Persistent,
            auto_create: true,
            max_cached_streams: 1000,
        }
    }
}

/// Stream service for managing stream storage and operations
pub struct StreamService<S: StorageAdaptor> {
    /// Service configuration
    config: StreamServiceConfig,

    /// Stream storage instances by stream name
    streams: Arc<RwLock<StreamStorageMap<S>>>,

    /// Stream configurations
    stream_configs: Arc<RwLock<HashMap<StreamName, StreamConfig>>>,

    /// Stream metadata
    stream_metadata: Arc<RwLock<HashMap<StreamName, StreamMetadata>>>,

    /// Storage manager for persistent streams
    storage_manager: Arc<StorageManager<S>>,

    /// Event service reference
    event_service: Arc<RwLock<Option<Arc<EventService>>>>,

    /// Service running state
    is_running: Arc<RwLock<bool>>,

    /// Shutdown signal
    shutdown: Arc<RwLock<Option<oneshot::Sender<()>>>>,

    /// Background task handles
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl<S: StorageAdaptor> StreamService<S> {
    /// Create a new stream service
    pub fn new(config: StreamServiceConfig, storage_manager: Arc<StorageManager<S>>) -> Self {
        Self {
            config,
            streams: Arc::new(RwLock::new(HashMap::new())),
            stream_configs: Arc::new(RwLock::new(HashMap::new())),
            stream_metadata: Arc::new(RwLock::new(HashMap::new())),
            storage_manager,
            event_service: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
            shutdown: Arc::new(RwLock::new(None)),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Set the event service for this service
    pub async fn set_event_service(&self, event_service: Arc<EventService>) {
        *self.event_service.write().await = Some(event_service);
    }

    /// Create a new stream with the given configuration
    pub async fn create_stream(
        &self,
        name: StreamName,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let mut configs = self.stream_configs.write().await;

        if configs.contains_key(&name) {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                format!("Stream {name} already exists"),
            ));
        }

        configs.insert(name.clone(), config.clone());

        // Initialize stream metadata
        let metadata = StreamMetadata {
            name: name.clone(),
            config: config.clone(),
            group_id,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_message_at: None,
            message_count: 0,
            total_bytes: 0,
        };
        self.stream_metadata
            .write()
            .await
            .insert(name.clone(), metadata);

        // Create storage if persistence is required
        if config.persistence_type == PersistenceType::Persistent {
            let namespace = proven_storage::StorageNamespace::new(format!("stream_{name}"));
            let storage = Arc::new(StreamStorageImpl::persistent(
                name.clone(),
                self.storage_manager.stream_storage(),
                namespace,
            ));

            self.streams.write().await.insert(name.clone(), storage);
        }

        info!(
            "Created stream {} in group {} with config {:?}",
            name, group_id, config
        );
        Ok(())
    }

    /// Delete a stream
    pub async fn delete_stream(&self, name: &StreamName) -> ConsensusResult<()> {
        self.stream_configs.write().await.remove(name);
        self.streams.write().await.remove(name);

        info!("Deleted stream {}", name);
        Ok(())
    }

    /// Get or create stream storage
    pub async fn get_or_create_storage(
        &self,
        stream_name: &StreamName,
        group_id: &str,
    ) -> Arc<StreamStorageImpl<StreamStorage<S>>> {
        let mut streams = self.streams.write().await;

        if let Some(storage) = streams.get(stream_name) {
            return storage.clone();
        }

        // Read configs to determine persistence type
        let configs = self.stream_configs.read().await;
        let persistence_type = configs
            .get(stream_name)
            .map(|c| c.persistence_type)
            .unwrap_or(self.config.default_persistence);
        drop(configs);

        let storage = match persistence_type {
            PersistenceType::Persistent => {
                let namespace = proven_storage::StorageNamespace::new(format!(
                    "stream_{group_id}_{stream_name}"
                ));
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

        streams.insert(stream_name.clone(), storage.clone());
        storage
    }

    /// Handle a stream event
    async fn handle_event(&self, envelope: EventEnvelope) -> ConsensusResult<()> {
        debug!("StreamService received event: {:?}", envelope.event);
        match &envelope.event {
            // StreamCreated events are now handled synchronously via GlobalConsensusCallbacks
            // which calls create_stream directly - no need to handle them via async events
            Event::StreamMessageAppended {
                stream,
                group_id,
                sequence,
                message,
                timestamp,
                term,
            } => {
                let stream_storage = self
                    .get_or_create_storage(stream, &group_id.to_string())
                    .await;

                stream_storage
                    .append(*sequence, message.clone(), *timestamp, *term)
                    .await
                    .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()))?;

                // Update metadata
                if let Some(metadata) = self.stream_metadata.write().await.get_mut(stream) {
                    metadata.last_message_at = Some(*timestamp);
                    metadata.message_count += 1;
                    metadata.total_bytes += message.payload.len() as u64;
                }

                info!(
                    "Stored message {} for stream {} in group {}",
                    sequence, stream, group_id
                );
            }
            Event::StreamTrimmed {
                stream,
                group_id,
                new_start_seq,
            } => {
                if let Some(_storage) = self.streams.read().await.get(stream) {
                    // Implement trimming when storage supports it
                    debug!(
                        "Stream {} in group {} trimmed to start at {}",
                        stream, group_id, new_start_seq
                    );
                }
            }
            Event::StreamMessageDeleted {
                stream,
                group_id,
                sequence,
                timestamp: _,
            } => {
                let _ = self
                    .get_or_create_storage(stream, &group_id.to_string())
                    .await;

                // Use the delete method which handles metadata updates
                match self.delete_message_from_storage(stream, *sequence).await {
                    Ok(deleted) => {
                        if !deleted {
                            debug!(
                                "Message {} not found in stream {} storage",
                                sequence, stream
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to delete message {} from stream {}: {}",
                            sequence, stream, e
                        );
                    }
                }
            }
            _ => {
                // Ignore other events
            }
        }

        Ok(())
    }

    /// Start the event processing loop
    async fn start_event_processing(&self, shutdown_rx: oneshot::Receiver<()>) {
        let event_service = match self.event_service.read().await.as_ref() {
            Some(service) => service.clone(),
            None => {
                error!("Event service not set, cannot start event processing");
                return;
            }
        };

        // Subscribe to stream events
        let filter = EventFilter::ByType(vec![EventType::Stream]);
        let mut subscriber = match event_service
            .subscribe("stream-service".to_string(), filter)
            .await
        {
            Ok(sub) => sub,
            Err(e) => {
                error!("Failed to subscribe to events: {}", e);
                return;
            }
        };

        let service = self.clone();

        let handle = tokio::spawn(async move {
            tokio::pin!(shutdown_rx);

            loop {
                tokio::select! {
                    Some(envelope) = subscriber.recv() => {
                        if let Err(e) = service.handle_event(envelope).await {
                            error!("Error handling stream event: {}", e);
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("Stream service event processing shutting down");
                        break;
                    }
                    else => {
                        info!("Stream service event channel closed");
                        break;
                    }
                }
            }
        });

        self.background_tasks.write().await.push(handle);
    }

    /// Get stream storage for reading
    pub async fn get_stream(
        &self,
        stream_name: &StreamName,
    ) -> Option<Arc<StreamStorageImpl<StreamStorage<S>>>> {
        self.streams.read().await.get(stream_name).cloned()
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_name: &StreamName) -> Option<StreamConfig> {
        self.stream_configs.read().await.get(stream_name).cloned()
    }

    /// Read messages from a stream
    pub async fn read_messages(
        &self,
        stream_name: &str,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        let stream_name = StreamName::new(stream_name);

        // Get the stream storage
        let stream_storage = self.get_stream(&stream_name).await.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream {stream_name} not found"),
            )
        })?;

        // Read the requested range
        let end_sequence = start_sequence.saturating_add(count);
        stream_storage
            .read_range(start_sequence, end_sequence)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()))
    }

    /// Stream messages from a stream
    ///
    /// Returns a stream of messages starting from the given sequence.
    /// If end_sequence is None, streams until the last available message.
    pub async fn stream_messages(
        &self,
        stream_name: &str,
        start_sequence: u64,
        end_sequence: Option<u64>,
    ) -> ConsensusResult<
        Pin<Box<dyn Stream<Item = ConsensusResult<crate::services::stream::StoredMessage>> + Send>>,
    > {
        let stream_name_str = stream_name.to_string();
        let stream_name = StreamName::new(stream_name);

        // Check if the stream exists
        let stream_config = self.get_stream_config(&stream_name).await.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream {stream_name} not found"),
            )
        })?;

        // For persistent streams, use storage streaming
        if stream_config.persistence_type == PersistenceType::Persistent {
            // Use proven_storage::LogStorageStreaming if available
            let namespace = StorageNamespace::new(format!("stream_{stream_name_str}"));
            let stream_storage = self.storage_manager.stream_storage();

            // Check if the storage supports streaming
            if let Ok(storage_stream) = proven_storage::LogStorageStreaming::stream_range(
                &stream_storage,
                &namespace,
                start_sequence,
                end_sequence,
            )
            .await
            {
                // Convert storage stream to StoredMessage stream
                let mapped_stream = storage_stream.map(move |result| {
                    result
                        .and_then(|(_seq, bytes)| {
                            crate::services::stream::deserialize_stored_message(bytes).map_err(
                                |e| proven_storage::StorageError::InvalidValue(e.to_string()),
                            )
                        })
                        .map_err(|e| Error::with_context(ErrorKind::Storage, e.to_string()))
                });

                return Ok(Box::pin(mapped_stream));
            }
        }

        // Fallback to batch reading for ephemeral streams or if streaming not supported
        let storage = self.get_stream(&stream_name).await.ok_or_else(|| {
            Error::with_context(
                ErrorKind::NotFound,
                format!("Stream storage {stream_name} not found"),
            )
        })?;

        // Create a stream that reads in batches
        let batch_size = 100;
        let stream = async_stream::stream! {
            let mut current_seq = start_sequence;

            loop {
                let batch_end = match end_sequence {
                    Some(end) => std::cmp::min(current_seq + batch_size, end),
                    None => current_seq + batch_size,
                };

                match storage.read_range(current_seq, batch_end).await {
                    Ok(messages) => {
                        if messages.is_empty() {
                            // No more messages
                            break;
                        }

                        for msg in messages {
                            current_seq = msg.sequence + 1;
                            yield Ok(msg);
                        }

                        // If we've reached the end sequence, stop
                        if let Some(end) = end_sequence && current_seq >= end {
                            break;
                        }
                    }
                    Err(e) => {
                        yield Err(Error::with_context(ErrorKind::Storage, e.to_string()));
                        break;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamName> {
        self.stream_configs.read().await.keys().cloned().collect()
    }

    /// Get stream metadata
    pub async fn get_stream_metadata(&self, stream_name: &StreamName) -> Option<StreamMetadata> {
        self.stream_metadata.read().await.get(stream_name).cloned()
    }

    /// Get all stream metadata
    pub async fn get_all_stream_metadata(&self) -> Vec<StreamMetadata> {
        self.stream_metadata
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Get the underlying storage manager
    pub fn storage_manager(&self) -> Arc<StorageManager<S>> {
        self.storage_manager.clone()
    }

    /// Delete a message from storage immediately (for consensus operations)
    pub async fn delete_message_from_storage(
        &self,
        stream_name: &StreamName,
        sequence: u64,
    ) -> ConsensusResult<bool> {
        let storage_namespace = StorageNamespace::new(format!("stream_{stream_name}"));
        let stream_storage = self.storage_manager.stream_storage();
        match stream_storage
            .delete_entry(&storage_namespace, sequence)
            .await
        {
            Ok(deleted) => {
                if deleted {
                    // Update metadata
                    if let Some(metadata) = self.stream_metadata.write().await.get_mut(stream_name)
                        && metadata.message_count > 0
                    {
                        metadata.message_count -= 1;
                    }
                    info!(
                        "Deleted message {} from stream {} storage",
                        sequence, stream_name
                    );
                }
                Ok(deleted)
            }
            Err(e) => {
                error!(
                    "Failed to delete message {} from stream {}: {}",
                    sequence, stream_name, e
                );
                Err(Error::with_context(
                    ErrorKind::Storage,
                    format!("Failed to delete message: {e}"),
                ))
            }
        }
    }

    /// Update stream metadata after appending a message (for consensus operations)
    pub async fn update_stream_metadata_for_append(
        &self,
        stream_name: &StreamName,
        timestamp: u64,
        message_size: u64,
    ) {
        if let Some(metadata) = self.stream_metadata.write().await.get_mut(stream_name) {
            metadata.last_message_at = Some(timestamp);
            metadata.message_count += 1;
            metadata.total_bytes += message_size;
        }
    }
}

// Implement Clone for StreamService to match other services
impl<S: StorageAdaptor> Clone for StreamService<S> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            streams: self.streams.clone(),
            stream_configs: self.stream_configs.clone(),
            stream_metadata: self.stream_metadata.clone(),
            storage_manager: self.storage_manager.clone(),
            event_service: self.event_service.clone(),
            is_running: self.is_running.clone(),
            shutdown: self.shutdown.clone(),
            background_tasks: self.background_tasks.clone(),
        }
    }
}

#[async_trait::async_trait]
impl<S: StorageAdaptor + 'static> ServiceLifecycle for StreamService<S> {
    async fn start(&self) -> ConsensusResult<()> {
        info!("Starting StreamService");

        // Check if already running
        if *self.is_running.read().await {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                "StreamService is already running",
            ));
        }

        // Check that event service is set
        if self.event_service.read().await.is_none() {
            return Err(Error::with_context(
                ErrorKind::Configuration,
                "Event service not set",
            ));
        }

        // Mark as running
        *self.is_running.write().await = true;

        // Set up shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self.shutdown.write().await = Some(shutdown_tx);

        // Start event processing
        self.start_event_processing(shutdown_rx).await;

        info!("StreamService started successfully");
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        info!("Stopping StreamService");

        // Check if running
        if !*self.is_running.read().await {
            return Ok(());
        }

        // Signal shutdown
        if let Some(shutdown_tx) = self.shutdown.write().await.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for background tasks to complete
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                warn!("Error waiting for background task: {}", e);
            }
        }

        // Clear all stream storage instances to release storage references
        self.streams.write().await.clear();
        self.stream_configs.write().await.clear();
        self.stream_metadata.write().await.clear();

        // Mark as not running
        *self.is_running.write().await = false;

        info!("StreamService stopped");
        Ok(())
    }

    async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        let is_running = self.is_running().await;
        let has_event_service = self.event_service.read().await.is_some();
        let stream_count = self.streams.read().await.len();

        let mut subsystems = vec![];

        // Check event service subsystem
        subsystems.push(SubsystemHealth {
            name: "event_service".to_string(),
            status: if has_event_service {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: if !has_event_service {
                Some("Event service not configured".to_string())
            } else {
                None
            },
        });

        // Check storage subsystem
        subsystems.push(SubsystemHealth {
            name: "storage".to_string(),
            status: HealthStatus::Healthy,
            message: Some(format!("{stream_count} streams loaded")),
        });

        let (status, message) = if !is_running {
            (
                HealthStatus::Unhealthy,
                Some("Service not running".to_string()),
            )
        } else if !has_event_service {
            (
                HealthStatus::Degraded,
                Some("Event service not available".to_string()),
            )
        } else {
            (HealthStatus::Healthy, None)
        };

        Ok(ServiceHealth {
            name: "StreamService".to_string(),
            status,
            message,
            subsystems,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::event::{EventConfig, EventService};
    use bytes::Bytes;
    use proven_storage::{LogStorage, StorageNamespace, StorageResult};

    // Create a dummy storage implementation for testing
    #[derive(Clone, Debug)]
    struct DummyStorage;

    #[async_trait::async_trait]
    impl LogStorage for DummyStorage {
        async fn append(
            &self,
            _namespace: &StorageNamespace,
            _entries: Vec<(u64, Bytes)>,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn bounds(&self, _namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>> {
            Ok(None)
        }

        async fn compact_before(
            &self,
            _namespace: &StorageNamespace,
            _index: u64,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn read_range(
            &self,
            _namespace: &StorageNamespace,
            _start: u64,
            _end: u64,
        ) -> StorageResult<Vec<(u64, Bytes)>> {
            Ok(vec![])
        }

        async fn truncate_after(
            &self,
            _namespace: &StorageNamespace,
            _index: u64,
        ) -> StorageResult<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl LogStorageWithDelete for DummyStorage {
        async fn delete_entry(
            &self,
            _namespace: &StorageNamespace,
            _index: u64,
        ) -> StorageResult<bool> {
            Ok(true)
        }
    }

    #[async_trait::async_trait]
    impl proven_storage::LogStorageStreaming for DummyStorage {
        async fn stream_range(
            &self,
            _namespace: &StorageNamespace,
            _start: u64,
            _end: Option<u64>,
        ) -> StorageResult<
            Box<dyn tokio_stream::Stream<Item = StorageResult<(u64, Bytes)>> + Send + Unpin>,
        > {
            Ok(Box::new(tokio_stream::empty()))
        }
    }

    impl StorageAdaptor for DummyStorage {}

    #[tokio::test]
    async fn test_stream_service_lifecycle() {
        let config = StreamServiceConfig::default();
        let storage_manager = Arc::new(StorageManager::new(DummyStorage));
        let service = StreamService::new(config, storage_manager);

        // Should not be running initially
        assert!(!service.is_running().await);

        // Should fail to start without event service
        assert!(service.start().await.is_err());

        // Set event service and start
        let event_config = EventConfig::default();
        let mut event_service = EventService::new(event_config);
        event_service.start().await.ok();
        service.set_event_service(Arc::new(event_service)).await;
        assert!(service.start().await.is_ok());
        assert!(service.is_running().await);

        // Should fail to start again
        assert!(service.start().await.is_err());

        // Stop service
        assert!(service.stop().await.is_ok());
        assert!(!service.is_running().await);
    }

    #[tokio::test]
    async fn test_stream_operations() {
        let config = StreamServiceConfig::default();
        let storage_manager = Arc::new(StorageManager::new(DummyStorage));
        let service = StreamService::new(config, storage_manager);

        let stream_name = StreamName::new("test-stream");
        let stream_config = StreamConfig {
            persistence_type: PersistenceType::Ephemeral,
            retention: RetentionPolicy::default(),
            max_message_size: 1024 * 1024,
            allow_auto_create: false,
        };

        // Create stream
        let group_id = ConsensusGroupId::new(1);
        assert!(
            service
                .create_stream(stream_name.clone(), stream_config.clone(), group_id)
                .await
                .is_ok()
        );

        // Should fail to create duplicate
        assert!(
            service
                .create_stream(stream_name.clone(), stream_config, group_id)
                .await
                .is_err()
        );

        // Should be in list
        let streams = service.list_streams().await;
        assert!(streams.contains(&stream_name));

        // Delete stream
        assert!(service.delete_stream(&stream_name).await.is_ok());

        // Should not be in list
        let streams = service.list_streams().await;
        assert!(!streams.contains(&stream_name));
    }
}
