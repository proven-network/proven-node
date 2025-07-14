//! Stream manager for handling per-stream storage instances
//!
//! This module manages individual storage instances for each stream,
//! enabling migration and per-stream configuration.

use super::per_stream_factory::{
    PerStreamStorageFactory, StreamPersistenceMode, StreamStorageBackend, UnifiedPerStreamFactory,
};
use super::traits::{
    CheckpointFormat, MessageData, StreamExport as TraitsStreamExport, StreamMetadata,
    StreamMetrics, StreamStorage,
};
use super::unified::UnifiedStreamStorage;
use crate::ConsensusGroupId;
use crate::config::stream::CompressionType;
use crate::config::{StorageType, StreamConfig};
use crate::core::group::migration::{
    CompressionType as MigrationCompressionType, MigrationCheckpoint, MigrationStreamMetadata,
};
use crate::error::{ConsensusResult, Error};
use crate::storage_backends::{
    StorageEngine, StorageKey, StorageValue,
    traits::{Priority, StorageHints},
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info};

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
    storage: Arc<UnifiedStreamStorage>,
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
    /// Underlying storage engine for stream metadata
    storage: Arc<UnifiedStreamStorage>,
    /// Maintenance task handle
    maintenance_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

// Namespaces for stream storage
mod namespaces {
    use crate::storage_backends::StorageNamespace;

    pub fn stream_metadata() -> StorageNamespace {
        StorageNamespace::new("stream_metadata")
    }

    pub fn stream_indexes() -> StorageNamespace {
        StorageNamespace::new("stream_indexes")
    }

    pub fn stream_checkpoints() -> StorageNamespace {
        StorageNamespace::new("stream_checkpoints")
    }

    pub fn stream_data(stream_name: &str) -> StorageNamespace {
        StorageNamespace::new(format!("stream_data_{stream_name}"))
    }

    pub fn migrations() -> StorageNamespace {
        StorageNamespace::new("migrations")
    }
}

// Keys for stream storage
mod keys {
    use crate::storage_backends::StorageKey;

    pub fn stream_metadata(stream_name: &str) -> StorageKey {
        StorageKey::from(format!("metadata_{stream_name}").as_str())
    }

    pub fn stream_message(stream_name: &str, sequence: u64) -> StorageKey {
        StorageKey::from(format!("msg_{stream_name}_{sequence:016x}").as_str())
    }

    pub fn migration_state(stream_name: &str) -> StorageKey {
        StorageKey::from(format!("migration_{stream_name}").as_str())
    }

    pub fn migration_checkpoint(stream_name: &str) -> StorageKey {
        StorageKey::from(format!("checkpoint_{stream_name}").as_str())
    }
}

/// Optimization strategy for stream operations
#[derive(Debug, Clone)]
pub struct StreamOptimizationStrategy {
    /// Use batch message writes
    pub batch_messages: bool,
    /// Use range scans for replay
    pub use_range_scan: bool,
    /// Stream large messages
    pub stream_large_messages: bool,
    /// Cache metadata
    pub cache_metadata: bool,
    /// Allow async acknowledgments
    pub allow_async_ack: bool,
    /// Maximum message size
    pub max_message_size: usize,
}

/// Migration strategy options
#[derive(Debug, Clone)]
pub enum MigrationStrategy {
    /// Stream-based migration for large streams
    Streaming,
    /// Range-based export
    RangeBased,
    /// Sequential export
    Sequential,
}

/// Stream health status
#[derive(Debug, Clone)]
pub enum StreamHealthStatus {
    /// Stream is healthy
    Healthy,
    /// Stream is idle
    Idle(Duration),
    /// Stream has performance issues
    Slow(String),
    /// Unknown status
    Unknown(String),
}

/// Migration state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    /// Stream name
    pub stream_name: String,
    /// Source group
    pub source_group: ConsensusGroupId,
    /// Target group
    pub target_group: ConsensusGroupId,
    /// Start time
    pub started_at: u64,
    /// Status
    pub status: MigrationStatus,
    /// Checkpoint sequence
    pub checkpoint_sequence: Option<u64>,
}

/// Migration status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Migration in progress
    InProgress,
    /// Migration completed
    Completed,
    /// Migration failed
    Failed(String),
}

/// Delivery guarantee for messages
#[derive(Debug, Clone, Copy)]
pub enum DeliveryGuarantee {
    /// At least once delivery
    AtLeastOnce,
    /// Exactly once delivery
    ExactlyOnce,
}

/// Stream message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    /// Message data
    pub data: Bytes,
    /// Headers
    pub headers: HashMap<String, String>,
}

/// Message entry in storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEntry {
    /// Sequence number
    pub sequence: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Message data
    pub data: Bytes,
    /// Headers
    pub headers: HashMap<String, String>,
}

/// Message stream for large messages
pub struct MessageStream {
    /// Sequence number
    pub sequence: u64,
    /// Reader for the data
    pub reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
}

/// Compaction result
#[derive(Debug, Default)]
pub struct CompactionResult {
    /// Number of entries compacted
    pub entries_compacted: u64,
    /// Bytes freed
    pub bytes_freed: u64,
}

use serde::{Deserialize, Serialize};

impl StreamManager {
    /// Create a new stream manager
    pub fn new(group_id: ConsensusGroupId, factory: Arc<UnifiedPerStreamFactory>) -> Self {
        // Create a metadata storage instance using the factory
        let storage = Arc::new(UnifiedStreamStorage::memory());

        Self {
            group_id,
            streams: Arc::new(RwLock::new(HashMap::new())),
            factory,
            storage,
            maintenance_handle: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Initialize stream storage with proper setup
    pub async fn initialize(&self) -> ConsensusResult<()> {
        // Initialize the underlying storage
        self.storage
            .initialize()
            .await
            .map_err(|e| Error::storage(format!("Failed to initialize stream storage: {e}")))?;

        // Pre-create common namespaces for better performance
        let namespaces = vec![
            namespaces::stream_metadata(),
            namespaces::stream_indexes(),
            namespaces::stream_checkpoints(),
        ];

        for ns in namespaces {
            self.storage
                .create_namespace(&ns)
                .await
                .map_err(|e| Error::storage(format!("Failed to create namespace: {e}")))?;
        }

        // Start background maintenance task
        self.start_maintenance_task().await;

        Ok(())
    }

    /// Shutdown stream storage gracefully
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        info!("Shutting down stream storage");

        // Stop maintenance task
        self.stop_maintenance_task().await;

        // Flush all pending writes
        self.storage
            .flush()
            .await
            .map_err(|e| Error::storage(format!("Failed to flush: {e}")))?;

        // Shutdown storage engine
        self.storage
            .shutdown()
            .await
            .map_err(|e| Error::storage(format!("Failed to shutdown: {e}")))?;

        Ok(())
    }

    /// Start the maintenance task
    async fn start_maintenance_task(&self) {
        let storage = self.storage.clone();
        let streams = self.streams.clone();
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match storage.maintenance().await {
                            Ok(result) => {
                                info!(
                                    bytes_reclaimed = result.bytes_reclaimed,
                                    entries_compacted = result.entries_compacted,
                                    "Stream storage maintenance completed"
                                );

                                // Also run stream-specific maintenance
                                if let Err(e) = Self::cleanup_deleted_streams_static(&storage, &streams).await {
                                    error!("Failed to cleanup deleted streams: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Stream storage maintenance failed: {}", e);
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Stopping maintenance task");
                        break;
                    }
                }
            }
        });

        *self.maintenance_handle.write().await = Some(handle);
    }

    /// Stop the maintenance task
    async fn stop_maintenance_task(&self) {
        self.shutdown.notify_one();

        if let Some(handle) = self.maintenance_handle.write().await.take() {
            let _ = handle.await;
        }
    }

    /// Cleanup deleted streams (static helper)
    async fn cleanup_deleted_streams_static(
        _storage: &Arc<UnifiedStreamStorage>,
        _streams: &Arc<RwLock<HashMap<String, ManagedStream>>>,
    ) -> ConsensusResult<()> {
        // This would scan for orphaned stream data and clean it up
        // For now, just return Ok
        Ok(())
    }

    /// Optimize stream operations based on storage capabilities
    pub fn get_optimization_strategy(&self) -> StreamOptimizationStrategy {
        let caps = self.storage.capabilities();

        StreamOptimizationStrategy {
            // Batch message writes if supported
            batch_messages: caps.atomic_batches,
            // Use range scans for stream replay
            use_range_scan: caps.efficient_range_scan,
            // Stream large messages directly
            stream_large_messages: caps.streaming,
            // Cache hot stream metadata
            cache_metadata: caps.caching,
            // Adjust consistency requirements
            allow_async_ack: caps.eventual_consistency,
            // Set message size limits
            max_message_size: caps.max_value_size.unwrap_or(usize::MAX),
        }
    }

    /// Choose best migration strategy based on capabilities
    pub async fn select_migration_strategy(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<MigrationStrategy> {
        let caps = self.storage.capabilities();
        let metrics = self.get_stream_metrics(stream_name).await?;

        if caps.streaming && metrics.disk_usage_bytes > 100 * 1024 * 1024 {
            // 100MB
            // Use streaming for large streams
            Ok(MigrationStrategy::Streaming)
        } else if caps.efficient_range_scan {
            // Use range-based export
            Ok(MigrationStrategy::RangeBased)
        } else {
            // Fall back to sequential export
            Ok(MigrationStrategy::Sequential)
        }
    }

    /// Create a new stream with its own storage instance
    pub async fn create_stream(
        &self,
        stream_id: String,
        config: StreamConfig,
    ) -> ConsensusResult<Arc<UnifiedStreamStorage>> {
        let mut streams = self.streams.write().await;

        // Check if stream already exists
        if streams.contains_key(&stream_id) {
            return Err(Error::already_exists(format!(
                "Stream {stream_id} already exists"
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
    ) -> ConsensusResult<Arc<UnifiedStreamStorage>> {
        let streams = self.streams.read().await;
        streams
            .get(stream_id)
            .map(|info| info.storage.clone())
            .ok_or_else(|| Error::not_found(format!("Stream {stream_id} not found")))
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
            .ok_or_else(|| Error::not_found(format!("Stream {stream_id} not found")))
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
            None => Err(Error::not_found(format!("Stream {stream_id} not found"))),
        }
    }

    /// Remove a stream and clean up its storage
    pub async fn remove_stream(&self, stream_id: &str) -> ConsensusResult<()> {
        let mut streams = self.streams.write().await;

        // Remove from map
        if streams.remove(stream_id).is_none() {
            return Err(Error::not_found(format!("Stream {stream_id} not found")));
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
            .ok_or_else(|| Error::not_found(format!("Stream {stream_id} not found")))?;

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
        _source_storage: Arc<UnifiedStreamStorage>,
    ) -> ConsensusResult<Arc<UnifiedStreamStorage>> {
        let mut streams = self.streams.write().await;

        // Check if stream already exists
        if streams.contains_key(&stream_id) {
            return Err(Error::already_exists(format!(
                "Stream {stream_id} already exists"
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

    /// Batch append messages to a stream
    pub async fn append_messages_batch(
        &self,
        stream_name: &str,
        messages: Vec<StreamMessage>,
    ) -> ConsensusResult<Vec<u64>> {
        let namespace = namespaces::stream_data(stream_name);

        // Get current sequence number
        let mut current_seq = self.get_last_sequence(stream_name).await?.unwrap_or(0);

        // Prepare batch with sequence numbers
        let mut sequences = Vec::with_capacity(messages.len());
        let mut batch_entries = Vec::with_capacity(messages.len());

        for message in messages {
            current_seq += 1;
            sequences.push(current_seq);

            let key = keys::stream_message(stream_name, current_seq);
            let entry = MessageEntry {
                sequence: current_seq,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                data: message.data,
                headers: message.headers,
            };

            batch_entries.push((key, StorageValue::new(serialize(&entry)?)));
        }

        // Batch write all messages
        let mut batch = self.storage.create_write_batch();
        for (key, value) in batch_entries {
            batch.put(namespace.clone(), key, value);
        }

        // Update metadata in same batch
        let metadata_key = keys::stream_metadata(stream_name);
        let mut metadata = self.get_metadata(stream_name).await?;
        metadata.last_sequence = current_seq;
        metadata.message_count += sequences.len() as u64;
        metadata.modified_at = chrono::Utc::now().timestamp_millis() as u64;

        batch.put(
            namespaces::stream_metadata(),
            metadata_key,
            StorageValue::new(serialize(&metadata)?),
        );

        // Apply batch atomically
        self.storage
            .write_batch(batch)
            .await
            .map_err(|e| Error::storage(format!("Batch write failed: {e}")))?;

        Ok(sequences)
    }

    /// Batch read messages for export/replay
    pub async fn read_messages_batch(
        &self,
        stream_name: &str,
        sequences: &[u64],
    ) -> ConsensusResult<Vec<Option<StreamMessage>>> {
        let namespace = namespaces::stream_data(stream_name);
        let keys: Vec<StorageKey> = sequences
            .iter()
            .map(|seq| keys::stream_message(stream_name, *seq))
            .collect();

        let values = self
            .storage
            .get_batch(&namespace, &keys)
            .await
            .map_err(|e| Error::storage(format!("Batch read failed: {e}")))?;

        // Convert to messages
        values
            .into_iter()
            .map(|opt_val| {
                opt_val
                    .map(|val| {
                        let entry: MessageEntry = deserialize(val.as_bytes())?;
                        Ok(StreamMessage {
                            data: entry.data,
                            headers: entry.headers,
                        })
                    })
                    .transpose()
            })
            .collect()
    }

    /// Append message with appropriate hints
    pub async fn append_message_with_hints(
        &self,
        stream_name: &str,
        message: StreamMessage,
        delivery_guarantee: DeliveryGuarantee,
    ) -> ConsensusResult<u64> {
        let namespace = namespaces::stream_data(stream_name);
        let sequence = self.next_sequence(stream_name).await?;
        let key = keys::stream_message(stream_name, sequence);

        // Stream messages are sequential writes
        let hints = match delivery_guarantee {
            DeliveryGuarantee::AtLeastOnce => StorageHints {
                is_batch_write: true,
                access_pattern: crate::storage_backends::traits::AccessPattern::Sequential,
                allow_eventual_consistency: true, // Can tolerate for this guarantee
                priority: Priority::Normal,
            },
            DeliveryGuarantee::ExactlyOnce => StorageHints {
                is_batch_write: true,
                access_pattern: crate::storage_backends::traits::AccessPattern::Sequential,
                allow_eventual_consistency: false, // Need strong consistency
                priority: Priority::High,
            },
        };

        let entry = MessageEntry {
            sequence,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            data: message.data,
            headers: message.headers,
        };

        let value = StorageValue::new(serialize(&entry)?);

        self.storage
            .put_with_hints(&namespace, key, value, hints)
            .await
            .map_err(|e| Error::storage(format!("Write failed: {e}")))?;

        Ok(sequence)
    }

    /// Create stream with metadata hints
    pub async fn create_stream_with_config(
        &self,
        stream_name: &str,
        config: StreamConfig,
    ) -> ConsensusResult<()> {
        // Stream metadata is critical and frequently accessed
        let hints = StorageHints {
            is_batch_write: false,
            access_pattern: crate::storage_backends::traits::AccessPattern::Random,
            allow_eventual_consistency: false,
            priority: Priority::Critical,
        };

        let metadata = StreamMetadata {
            name: stream_name.to_string(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            modified_at: chrono::Utc::now().timestamp_millis() as u64,
            last_sequence: 0,
            message_count: 0,
            config,
            is_paused: false,
            custom_metadata: HashMap::new(),
        };

        self.storage
            .put_with_hints(
                &namespaces::stream_metadata(),
                keys::stream_metadata(stream_name),
                StorageValue::new(serialize(&metadata)?),
                hints,
            )
            .await
            .map_err(|e| Error::storage(format!("Create stream failed: {e}")))?;

        Ok(())
    }

    /// Helper methods for sequence management
    async fn get_last_sequence(&self, stream_name: &str) -> ConsensusResult<Option<u64>> {
        let metadata = self.get_metadata(stream_name).await?;
        Ok(Some(metadata.last_sequence))
    }

    async fn next_sequence(&self, stream_name: &str) -> ConsensusResult<u64> {
        let last = self.get_last_sequence(stream_name).await?.unwrap_or(0);
        Ok(last + 1)
    }

    async fn get_metadata(&self, stream_name: &str) -> ConsensusResult<StreamMetadata> {
        let key = keys::stream_metadata(stream_name);
        let namespace = namespaces::stream_metadata();

        match self.storage.get(&namespace, &key).await {
            Ok(Some(value)) => {
                let metadata = deserialize(value.as_bytes())?;
                Ok(metadata)
            }
            Ok(None) => Err(Error::not_found(format!("Stream {stream_name} not found"))),
            Err(e) => Err(Error::storage(format!("Failed to get metadata: {e}"))),
        }
    }

    /// Atomically start migration if not already in progress
    pub async fn start_migration_atomic(
        &self,
        stream_name: &str,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<bool> {
        let namespace = namespaces::migrations();
        let key = keys::migration_state(stream_name);

        let migration_state = MigrationState {
            stream_name: stream_name.to_string(),
            source_group: self.group_id,
            target_group,
            started_at: chrono::Utc::now().timestamp_millis() as u64,
            status: MigrationStatus::InProgress,
            checkpoint_sequence: None,
        };

        let value = StorageValue::new(serialize(&migration_state)?);

        // Only succeed if no migration exists
        let started = self
            .storage
            .put_if_absent(&namespace, key, value)
            .await
            .map_err(|e| Error::storage(format!("Failed to start migration: {e}")))?;

        if started {
            info!(
                stream = %stream_name,
                target = ?target_group,
                "Migration started atomically"
            );
        }

        Ok(started)
    }

    /// Update migration checkpoint atomically
    pub async fn update_migration_checkpoint(
        &self,
        stream_name: &str,
        old_checkpoint: Option<u64>,
        new_checkpoint: u64,
    ) -> ConsensusResult<bool> {
        let namespace = namespaces::migrations();
        let key = keys::migration_checkpoint(stream_name);

        let old_val = old_checkpoint.map(|cp| StorageValue::new(cp.to_be_bytes().to_vec()));
        let new_val = StorageValue::new(new_checkpoint.to_be_bytes().to_vec());

        let updated = self
            .storage
            .compare_and_swap(&namespace, &key, old_val.as_ref(), new_val)
            .await
            .map_err(|e| Error::storage(format!("Failed to update checkpoint: {e}")))?;

        Ok(updated)
    }

    /// Get message count for a stream
    #[allow(dead_code)]
    async fn get_message_count(&self, stream_name: &str) -> ConsensusResult<u64> {
        let metadata = self.get_metadata(stream_name).await?;
        Ok(metadata.message_count)
    }

    /// Check stream health
    pub async fn check_stream_health(&self, stream_name: &str) -> StreamHealthStatus {
        let metrics = match self.get_stream_metrics(stream_name).await {
            Ok(m) => m,
            Err(e) => return StreamHealthStatus::Unknown(e.to_string()),
        };

        // Check for stale streams
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if let Some(newest) = metrics.newest_message_timestamp {
            let idle_time = now - newest;
            if idle_time > 86400000 {
                // 24 hours
                return StreamHealthStatus::Idle(Duration::from_millis(idle_time));
            }
        }

        // Check performance (would need actual latency tracking)
        // For now, just return healthy
        StreamHealthStatus::Healthy
    }

    /// Calculate read rate for a stream
    async fn calculate_read_rate(&self, _stream_name: &str) -> ConsensusResult<f64> {
        // This would track actual read operations
        // For now, return a placeholder
        Ok(0.0)
    }

    /// Calculate write rate for a stream
    async fn calculate_write_rate(&self, _stream_name: &str) -> ConsensusResult<f64> {
        // This would track actual write operations
        // For now, return a placeholder
        Ok(0.0)
    }
}

// Implement StreamStorage trait
#[async_trait]
impl StreamStorage for StreamManager {
    async fn export_stream(&self, stream_name: &str) -> ConsensusResult<TraitsStreamExport> {
        let metadata = self.get_metadata(stream_name).await?;
        let namespace = namespaces::stream_data(stream_name);

        // Read all messages
        let mut messages = Vec::new();
        for seq in 1..=metadata.last_sequence {
            let key = keys::stream_message(stream_name, seq);
            if let Ok(Some(value)) = self.storage.get(&namespace, &key).await {
                let entry: MessageEntry = deserialize(value.as_bytes())?;
                messages.push((
                    seq,
                    MessageData {
                        data: entry.data,
                        metadata: Some(entry.headers.clone()),
                        timestamp: entry.timestamp,
                    },
                ));
            }
        }

        Ok(TraitsStreamExport {
            metadata,
            messages,
            checkpoint_format: CheckpointFormat::Full,
            compression: CompressionType::None,
            exported_at: chrono::Utc::now().timestamp_millis() as u64,
            source_group: self.group_id,
        })
    }

    async fn import_stream(&self, export: TraitsStreamExport) -> ConsensusResult<()> {
        // Create the stream
        self.create_stream_with_config(&export.metadata.name, export.metadata.config.clone())
            .await?;

        // Import all messages
        let namespace = namespaces::stream_data(&export.metadata.name);
        let mut batch = self.storage.create_write_batch();

        for (seq, msg_data) in export.messages {
            let key = keys::stream_message(&export.metadata.name, seq);
            let entry = MessageEntry {
                sequence: seq,
                timestamp: msg_data.timestamp,
                data: msg_data.data,
                headers: msg_data.metadata.unwrap_or_default(),
            };
            batch.put(
                namespace.clone(),
                key,
                StorageValue::new(serialize(&entry)?),
            );
        }

        self.storage
            .write_batch(batch)
            .await
            .map_err(|e| Error::storage(format!("Import failed: {e}")))?;

        Ok(())
    }

    async fn create_checkpoint(
        &self,
        stream_name: &str,
        since_seq: Option<u64>,
    ) -> ConsensusResult<MigrationCheckpoint> {
        let metadata = self.get_metadata(stream_name).await?;
        let namespace = namespaces::stream_data(stream_name);

        let start_seq = since_seq.map(|s| s + 1).unwrap_or(1);
        let mut messages = Vec::new();

        for seq in start_seq..=metadata.last_sequence {
            let key = keys::stream_message(stream_name, seq);
            if let Ok(Some(value)) = self.storage.get(&namespace, &key).await {
                let entry: MessageEntry = deserialize(value.as_bytes())?;
                messages.push((seq, serialize(&entry)?));
            }
        }

        Ok(MigrationCheckpoint {
            stream_name: stream_name.to_string(),
            sequence: metadata.last_sequence,
            storage_type: match metadata.config.storage_type {
                StorageType::Memory => crate::core::group::migration::StorageType::Memory,
                StorageType::File => crate::core::group::migration::StorageType::RocksDB,
            },
            config: metadata.config.clone(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum: String::new(), // Would calculate actual checksum
            compression: match metadata.config.compression {
                CompressionType::None => MigrationCompressionType::None,
                CompressionType::Lz4 => MigrationCompressionType::Lz4,
                _ => MigrationCompressionType::Gzip, // Default to Gzip for others
            },
            is_incremental: since_seq.is_some(),
            base_checkpoint_seq: since_seq,
            message_count: messages.len() as u64,
            total_bytes: messages.iter().map(|(_, data)| data.len()).sum::<usize>() as u64,
            stream_metadata: MigrationStreamMetadata {
                is_paused: metadata.is_paused,
                paused_at: if metadata.is_paused {
                    Some(chrono::Utc::now().timestamp_millis() as u64)
                } else {
                    None
                },
                has_pending_operations: false,
            },
        })
    }

    async fn apply_checkpoint(&self, checkpoint: &MigrationCheckpoint) -> ConsensusResult<()> {
        // For now, just create the stream with the checkpoint config
        // In a real implementation, we would restore the actual messages
        let config = StreamConfig {
            max_messages: checkpoint.config.max_messages,
            max_bytes: checkpoint.config.max_bytes,
            max_age_secs: checkpoint.config.max_age_secs,
            storage_type: checkpoint.config.storage_type,
            retention_policy: checkpoint.config.retention_policy,
            pubsub_bridge_enabled: checkpoint.config.pubsub_bridge_enabled,
            consensus_group: checkpoint.config.consensus_group,
            compact_on_deletion: checkpoint.config.compact_on_deletion,
            compression: checkpoint.config.compression,
        };

        self.create_stream_with_config(&checkpoint.stream_name, config)
            .await?;

        Ok(())
    }

    async fn get_stream_metrics(&self, stream_name: &str) -> ConsensusResult<StreamMetrics> {
        let metadata = self.get_metadata(stream_name).await?;
        let namespace = namespaces::stream_data(stream_name);
        let size = self
            .storage
            .namespace_size(&namespace)
            .await
            .map_err(|e| Error::storage(format!("Failed to get size: {e}")))?;

        // Get first and last message timestamps
        let oldest_timestamp = if metadata.message_count > 0 {
            let key = keys::stream_message(stream_name, 1);
            if let Ok(Some(value)) = self.storage.get(&namespace, &key).await {
                let entry: MessageEntry = deserialize(value.as_bytes())?;
                Some(entry.timestamp)
            } else {
                None
            }
        } else {
            None
        };

        let newest_timestamp = if metadata.last_sequence > 0 {
            let key = keys::stream_message(stream_name, metadata.last_sequence);
            if let Ok(Some(value)) = self.storage.get(&namespace, &key).await {
                let entry: MessageEntry = deserialize(value.as_bytes())?;
                Some(entry.timestamp)
            } else {
                None
            }
        } else {
            None
        };

        Ok(StreamMetrics {
            stream_name: stream_name.to_string(),
            disk_usage_bytes: size,
            message_count: metadata.message_count,
            oldest_message_timestamp: oldest_timestamp,
            newest_message_timestamp: newest_timestamp,
            write_rate: self.calculate_write_rate(stream_name).await?,
            read_rate: self.calculate_read_rate(stream_name).await?,
        })
    }

    async fn stream_exists(&self, stream_name: &str) -> ConsensusResult<bool> {
        Ok(self.stream_exists(stream_name).await)
    }

    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<()> {
        self.remove_stream(stream_name).await
    }
}

/// Serialize helper
fn serialize<T: Serialize>(value: &T) -> ConsensusResult<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::into_writer(value, &mut buf)
        .map_err(|e| Error::storage(format!("Serialize failed: {e}")))?;
    Ok(buf)
}

/// Deserialize helper
fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> ConsensusResult<T> {
    ciborium::from_reader(data).map_err(|e| Error::storage(format!("Deserialize failed: {e}")))
}

/// Stream export data for migration
pub struct StreamExport {
    /// Stream identifier
    pub stream_id: String,
    /// Stream configuration
    pub config: StreamConfig,
    /// Storage instance
    pub storage: Arc<UnifiedStreamStorage>,
    /// Source group ID
    pub group_id: ConsensusGroupId,
}

#[cfg(test)]
mod tests {
    use crate::config::{RetentionPolicy, StorageType};

    use super::*;

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
