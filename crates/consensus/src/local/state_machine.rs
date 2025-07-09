//! Storage-backed state machine for local consensus groups
//!
//! This module provides a state machine implementation that uses per-stream
//! storage instances. Each stream gets its own isolated storage backend,
//! enabling easier migration and per-stream configuration.

use crate::{
    allocation::ConsensusGroupId,
    local::stream_storage::{
        log_types::{MessageSource, StreamLogEntry, StreamMetadata as StreamLogMetadata},
        stream_manager::UnifiedStreamManager,
        traits::StreamConfig,
    },
    storage::{StorageEngine, StorageIterator, StorageKey, StorageValue, keys, log::LogStorage},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Storage-backed state machine for local consensus groups with per-stream storage
#[derive(Clone)]
pub struct StorageBackedLocalState {
    /// Group ID for this state machine
    group_id: ConsensusGroupId,
    /// Stream manager that handles per-stream storage instances
    stream_manager: Arc<UnifiedStreamManager>,
    /// Cached metrics (updated periodically)
    cached_metrics: Arc<RwLock<CachedMetrics>>,
}

/// Cached metrics to avoid frequent storage queries
#[derive(Default, Clone)]
struct CachedMetrics {
    /// Total messages across all streams
    total_messages: u64,
    /// Total bytes stored
    total_bytes: u64,
    /// Last update timestamp
    #[allow(dead_code)]
    last_update: u64,
}

/// Stream metadata stored in the stream's metadata namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Last sequence number
    pub last_seq: u64,
    /// Whether the stream is paused for migration
    pub is_paused: bool,
    /// Pause timestamp for migration coordination
    pub paused_at: Option<u64>,
    /// Number of messages in the stream
    pub message_count: u64,
    /// Total bytes in the stream
    pub total_bytes: u64,
}

/// Pending operation stored in storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPendingOperation {
    /// Operation ID (timestamp-based)
    pub id: u64,
    /// Operation type
    pub operation_type: PendingOperationType,
    /// Data for the operation
    pub data: Bytes,
    /// Metadata
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp when operation was attempted
    pub timestamp: u64,
}

/// Types of operations that can be pending
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PendingOperationType {
    /// Publish message operation
    Publish,
    /// Rollup operation
    Rollup,
    /// Delete message operation
    Delete {
        /// Sequence number of message to delete
        sequence: u64,
    },
}

/// Namespace constants for per-stream storage
mod namespaces {
    use crate::storage::StorageNamespace;

    /// Messages namespace within a stream's storage
    pub fn messages() -> StorageNamespace {
        StorageNamespace::new("messages")
    }

    /// Metadata namespace within a stream's storage
    pub fn metadata() -> StorageNamespace {
        StorageNamespace::new("metadata")
    }

    /// Pending operations namespace within a stream's storage
    pub fn pending() -> StorageNamespace {
        StorageNamespace::new("pending")
    }
}

impl StorageBackedLocalState {
    /// Create a new storage-backed state machine with per-stream storage
    pub fn new(group_id: ConsensusGroupId, stream_manager: Arc<UnifiedStreamManager>) -> Self {
        Self {
            group_id,
            stream_manager,
            cached_metrics: Arc::new(RwLock::new(CachedMetrics::default())),
        }
    }

    /// Get stream metadata
    async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>, String> {
        let storage = match self.stream_manager.get_stream_storage(stream_id).await {
            Ok(storage) => storage,
            Err(_) => return Ok(None), // Stream doesn't exist
        };

        let key = StorageKey::from("stream_meta");
        match storage
            .get(&namespaces::metadata(), &key)
            .await
            .map_err(|e| format!("Storage error: {}", e))?
        {
            Some(value) => {
                let metadata: StreamMetadata = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| format!("Failed to deserialize metadata: {}", e))?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Set stream metadata
    async fn set_stream_metadata(
        &self,
        stream_id: &str,
        metadata: &StreamMetadata,
    ) -> Result<(), String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        let key = StorageKey::from("stream_meta");
        let mut buffer = Vec::new();
        ciborium::into_writer(metadata, &mut buffer)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;

        storage
            .put(&namespaces::metadata(), key, StorageValue::new(buffer))
            .await
            .map_err(|e| format!("Storage error: {}", e))
    }

    /// Create a new stream with its own storage instance
    pub async fn create_stream(&self, stream_id: &str, config: StreamConfig) -> Result<(), String> {
        // Stream manager handles duplicate checking
        self.stream_manager
            .create_stream(stream_id.to_string(), config)
            .await
            .map_err(|e| e.to_string())?;

        // Initialize stream metadata
        let metadata = StreamMetadata {
            last_seq: 0,
            is_paused: false,
            paused_at: None,
            message_count: 0,
            total_bytes: 0,
        };

        self.set_stream_metadata(stream_id, &metadata).await?;

        // Create namespaces in the stream's storage
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        // Create the necessary namespaces
        storage
            .create_namespace(&namespaces::messages())
            .await
            .map_err(|e| format!("Failed to create messages namespace: {}", e))?;

        storage
            .create_namespace(&namespaces::metadata())
            .await
            .map_err(|e| format!("Failed to create metadata namespace: {}", e))?;

        storage
            .create_namespace(&namespaces::pending())
            .await
            .map_err(|e| format!("Failed to create pending namespace: {}", e))?;

        Ok(())
    }

    /// Publish a message to a stream
    pub async fn publish_message(
        &mut self,
        stream_id: &str,
        data: Bytes,
        headers: Option<HashMap<String, String>>,
    ) -> Result<u64, String> {
        // Get stream's storage
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| format!("Stream {} not found: {}", stream_id, e))?;

        // Get current metadata
        let mut metadata = self
            .get_stream_metadata(stream_id)
            .await?
            .ok_or_else(|| format!("Stream {} not found", stream_id))?;

        // Check if stream is paused
        if metadata.is_paused {
            // Store as pending operation
            let pending_op = StoredPendingOperation {
                id: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                operation_type: PendingOperationType::Publish,
                data: data.clone(),
                metadata: headers.clone(),
                timestamp: chrono::Utc::now().timestamp() as u64,
            };

            self.store_pending_operation(stream_id, &pending_op).await?;
            return Err("Stream is paused for migration".to_string());
        }

        // Increment sequence number
        metadata.last_seq += 1;
        let sequence = metadata.last_seq;

        // Create log entry metadata
        let log_metadata = StreamLogMetadata {
            headers: headers.unwrap_or_default(),
            compression: None,
            source: MessageSource::Consensus,
        };

        // Create LogStorage entry
        let log_entry = crate::storage::log::LogEntry {
            index: sequence,
            timestamp: chrono::Utc::now().timestamp() as u64,
            data: data.clone(),
            metadata: log_metadata,
        };

        // Use LogStorage to append the entry
        storage
            .append_entry(&namespaces::messages(), log_entry)
            .await
            .map_err(|e| format!("Storage error: {}", e))?;

        // Update metadata
        metadata.message_count += 1;
        metadata.total_bytes += data.len() as u64;
        self.set_stream_metadata(stream_id, &metadata).await?;

        // Update cached metrics
        let mut metrics = self.cached_metrics.write().await;
        metrics.total_messages += 1;
        metrics.total_bytes += data.len() as u64;

        Ok(sequence)
    }

    /// Delete a message from a stream
    pub async fn delete_message(&mut self, stream_id: &str, sequence: u64) -> Result<(), String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        let key = keys::encode_log_key(sequence);
        storage
            .delete(&namespaces::messages(), &key)
            .await
            .map_err(|e| format!("Storage error: {}", e))
    }

    /// Pause a stream for migration
    pub async fn pause_stream(&mut self, stream_id: &str) -> Result<(), String> {
        let mut metadata = self
            .get_stream_metadata(stream_id)
            .await?
            .ok_or_else(|| format!("Stream {} not found", stream_id))?;

        if metadata.is_paused {
            return Err("Stream is already paused".to_string());
        }

        metadata.is_paused = true;
        metadata.paused_at = Some(chrono::Utc::now().timestamp() as u64);

        self.set_stream_metadata(stream_id, &metadata).await
    }

    /// Resume a stream after migration
    pub async fn resume_stream(&mut self, stream_id: &str) -> Result<Vec<u64>, String> {
        let mut metadata = self
            .get_stream_metadata(stream_id)
            .await?
            .ok_or_else(|| format!("Stream {} not found", stream_id))?;

        if !metadata.is_paused {
            return Err("Stream is not paused".to_string());
        }

        metadata.is_paused = false;
        metadata.paused_at = None;

        // Process pending operations
        let pending_ops = self.get_pending_operations(stream_id).await?;
        let mut applied_sequences = Vec::new();

        for op in pending_ops {
            match op.operation_type {
                PendingOperationType::Publish => {
                    if let Ok(seq) = self.publish_message(stream_id, op.data, op.metadata).await {
                        applied_sequences.push(seq);
                    }
                }
                PendingOperationType::Delete { sequence } => {
                    self.delete_message(stream_id, sequence).await?;
                }
                PendingOperationType::Rollup => {
                    // Implement rollup operation if needed
                }
            }
        }

        // Clear pending operations
        self.clear_pending_operations(stream_id).await?;

        self.set_stream_metadata(stream_id, &metadata).await?;

        Ok(applied_sequences)
    }

    /// Store a pending operation
    async fn store_pending_operation(
        &self,
        stream_id: &str,
        op: &StoredPendingOperation,
    ) -> Result<(), String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        let key = StorageKey::from(format!("pending:{}", op.id).as_str());
        let mut buffer = Vec::new();
        ciborium::into_writer(op, &mut buffer)
            .map_err(|e| format!("Failed to serialize pending op: {}", e))?;

        storage
            .put(&namespaces::pending(), key, StorageValue::new(buffer))
            .await
            .map_err(|e| format!("Storage error: {}", e))
    }

    /// Get pending operations for a stream
    async fn get_pending_operations(
        &self,
        stream_id: &str,
    ) -> Result<Vec<StoredPendingOperation>, String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        let mut operations = Vec::new();
        let mut iter = storage
            .iter(&namespaces::pending())
            .await
            .map_err(|e| format!("Failed to create iterator: {}", e))?;

        while let Some((_, value)) = iter.next().map_err(|e| format!("Iterator error: {}", e))? {
            let op: StoredPendingOperation = ciborium::from_reader(value.as_bytes())
                .map_err(|e| format!("Failed to deserialize pending op: {}", e))?;
            operations.push(op);
        }

        Ok(operations)
    }

    /// Clear all pending operations
    async fn clear_pending_operations(&self, stream_id: &str) -> Result<(), String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        // Collect keys to delete
        let mut keys_to_delete = Vec::new();
        let mut iter = storage
            .iter(&namespaces::pending())
            .await
            .map_err(|e| format!("Failed to create iterator: {}", e))?;

        while let Some((key, _)) = iter.next().map_err(|e| format!("Iterator error: {}", e))? {
            keys_to_delete.push(key);
        }

        // Delete all pending operations
        for key in keys_to_delete {
            storage
                .delete(&namespaces::pending(), &key)
                .await
                .map_err(|e| format!("Storage error: {}", e))?;
        }

        Ok(())
    }

    /// Create a migration checkpoint metadata (actual data is streamed separately)
    pub async fn create_checkpoint_metadata(
        &self,
        stream_id: &str,
    ) -> Result<crate::migration::MigrationCheckpoint, String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        let metadata = self
            .get_stream_metadata(stream_id)
            .await?
            .ok_or_else(|| format!("Stream {} not found", stream_id))?;

        // Detect storage type from the actual storage backend
        let storage_type = match storage.storage_type() {
            "memory" => crate::migration::StorageType::Memory,
            "rocksdb" => crate::migration::StorageType::RocksDB,
            _ => crate::migration::StorageType::Memory, // Default fallback
        };

        // Get stream config and convert to global StreamConfig
        let local_config = self.get_stream_config(stream_id).await.unwrap_or_default();
        let stream_config = StreamConfig {
            max_messages: local_config.max_messages,
            max_bytes: local_config.max_bytes,
            max_age_secs: local_config.max_age_secs,
            storage_type: match storage_type {
                crate::migration::StorageType::Memory => {
                    crate::local::stream_storage::traits::StorageType::Memory
                }
                crate::migration::StorageType::RocksDB => {
                    crate::local::stream_storage::traits::StorageType::File
                }
                crate::migration::StorageType::S3 => {
                    crate::local::stream_storage::traits::StorageType::File
                }
            },
            retention_policy: local_config.retention_policy,
            pubsub_bridge_enabled: local_config.pubsub_bridge_enabled,
            consensus_group: local_config.consensus_group.or(Some(self.group_id)),
            compact_on_deletion: local_config.compact_on_deletion,
            compression: local_config.compression,
        };

        // Check for pending operations
        let has_pending = !self.get_pending_operations(stream_id).await?.is_empty();

        Ok(crate::migration::MigrationCheckpoint {
            stream_name: stream_id.to_string(),
            sequence: metadata.last_seq,
            storage_type,
            config: stream_config,
            subscriptions: Vec::new(), // Could be tracked in global state
            created_at: chrono::Utc::now().timestamp() as u64,
            checksum: String::new(), // Will be calculated during streaming
            compression: crate::migration::CompressionType::None,
            is_incremental: false,
            base_checkpoint_seq: None,
            message_count: metadata.message_count,
            total_bytes: metadata.total_bytes,
            stream_metadata: crate::migration::StreamMetadata {
                is_paused: metadata.is_paused,
                paused_at: metadata.paused_at,
                has_pending_operations: has_pending,
            },
        })
    }

    /// Create an iterator for exporting stream messages
    pub async fn create_export_iterator(
        &self,
        stream_id: &str,
        start_seq: u64,
        end_seq: u64,
    ) -> Result<
        crate::local::stream_storage::migration_iterator::MigrationMessageIterator<
            crate::storage::generic::GenericStorage,
        >,
        String,
    > {
        use crate::local::stream_storage::migration_iterator::MigrationMessageIterator;

        let storage = self
            .stream_manager
            .get_stream_storage(stream_id)
            .await
            .map_err(|e| e.to_string())?;

        MigrationMessageIterator::new(storage, namespaces::messages(), start_seq, end_seq)
            .await
            .map_err(|e| e.to_string())
    }

    /// Apply checkpoint metadata (prepare for receiving streamed data)
    pub async fn prepare_checkpoint_apply(
        &mut self,
        checkpoint: &crate::migration::MigrationCheckpoint,
    ) -> Result<(), String> {
        // Ensure stream exists with appropriate config
        if !self
            .stream_manager
            .stream_exists(&checkpoint.stream_name)
            .await
        {
            // Create stream with config from checkpoint
            let config = StreamConfig {
                max_age_secs: checkpoint.config.max_age_secs,
                max_messages: checkpoint.config.max_messages,
                max_bytes: checkpoint.config.max_bytes,
                storage_type: checkpoint.config.storage_type,
                retention_policy: checkpoint.config.retention_policy,
                pubsub_bridge_enabled: checkpoint.config.pubsub_bridge_enabled,
                consensus_group: checkpoint.config.consensus_group,
                compact_on_deletion: false,
                compression: crate::local::stream_storage::CompressionType::None,
            };

            self.create_stream(&checkpoint.stream_name, config).await?;
        }

        // If stream is currently not paused but checkpoint says it should be, pause it
        if checkpoint.stream_metadata.is_paused {
            self.pause_stream(&checkpoint.stream_name).await?;
        }

        Ok(())
    }

    /// Apply a batch of messages from migration
    pub async fn apply_message_batch(
        &mut self,
        stream_name: &str,
        messages: Vec<crate::migration::StreamMessage>,
    ) -> Result<(), String> {
        let storage = self
            .stream_manager
            .get_stream_storage(stream_name)
            .await
            .map_err(|e| e.to_string())?;

        // Use batch operations for efficiency
        let mut batch = crate::storage::WriteBatch::new();
        let mut _total_bytes = 0u64;

        for msg in messages {
            // Convert migration message to storage format
            let log_entry = StreamLogEntry {
                index: msg.sequence,
                timestamp: msg.timestamp,
                data: msg.data.clone(),
                metadata: StreamLogMetadata {
                    headers: msg.headers,
                    compression: msg.compression.as_ref().map(|c| match c.as_str() {
                        "gzip" => crate::local::stream_storage::log_types::CompressionType::Gzip,
                        "zstd" => crate::local::stream_storage::log_types::CompressionType::Zstd,
                        _ => crate::local::stream_storage::log_types::CompressionType::None,
                    }),
                    source: match msg.source {
                        crate::migration::MessageSourceType::Consensus => MessageSource::Consensus,
                        crate::migration::MessageSourceType::PubSub { subject, publisher } => {
                            MessageSource::PubSub { subject, publisher }
                        }
                        crate::migration::MessageSourceType::Migration { source_group } => {
                            MessageSource::Migration { source_group }
                        }
                    },
                },
            };

            let key = keys::encode_log_key(msg.sequence);
            let mut buffer = Vec::new();
            ciborium::into_writer(&log_entry, &mut buffer)
                .map_err(|e| format!("Failed to serialize entry: {}", e))?;

            batch.put(namespaces::messages(), key, StorageValue::new(buffer));
            _total_bytes += log_entry.data.len() as u64;
        }

        // Apply batch
        storage
            .write_batch(batch)
            .await
            .map_err(|e| format!("Storage error: {}", e))?;

        Ok(())
    }

    /// Finalize checkpoint application
    pub async fn finalize_checkpoint_apply(
        &mut self,
        checkpoint: &crate::migration::MigrationCheckpoint,
        actual_checksum: &str,
    ) -> Result<(), String> {
        // Verify checksum if provided
        if !checkpoint.checksum.is_empty() && checkpoint.checksum != actual_checksum {
            return Err(format!(
                "Checksum mismatch: expected {}, got {}",
                checkpoint.checksum, actual_checksum
            ));
        }

        // Update stream metadata
        let metadata = StreamMetadata {
            last_seq: checkpoint.sequence,
            is_paused: checkpoint.stream_metadata.is_paused,
            paused_at: checkpoint.stream_metadata.paused_at,
            message_count: checkpoint.message_count,
            total_bytes: checkpoint.total_bytes,
        };

        self.set_stream_metadata(&checkpoint.stream_name, &metadata)
            .await
    }

    /// Remove a stream
    pub async fn remove_stream(&mut self, stream_id: &str) -> Result<(), String> {
        // Stream manager handles all cleanup
        self.stream_manager
            .remove_stream(stream_id)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get metrics
    pub async fn get_metrics(&self) -> crate::local::LocalStateMetrics {
        let metrics = self.cached_metrics.read().await;
        let stream_count = self.stream_manager.stream_count().await;
        let stream_list = self.stream_manager.list_streams().await;

        // Build stream metrics map
        let mut stream_metrics = HashMap::new();
        for stream_name in stream_list {
            if let Ok(Some(metadata)) = self.get_stream_metadata(&stream_name).await {
                stream_metrics.insert(
                    stream_name,
                    crate::local::StreamMetrics {
                        message_count: metadata.message_count,
                        last_sequence: metadata.last_seq,
                        total_bytes: metadata.total_bytes,
                    },
                );
            }
        }

        crate::local::LocalStateMetrics {
            group_id: Some(self.group_id),
            stream_count: stream_count as u32,
            total_messages: metrics.total_messages,
            total_bytes: metrics.total_bytes,
            message_rate: 0.0, // TODO: Calculate actual message rate
            streams: stream_metrics,
        }
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<String> {
        self.stream_manager.list_streams().await
    }

    /// Get stream configuration
    pub async fn get_stream_config(&self, stream_id: &str) -> Result<StreamConfig, String> {
        self.stream_manager
            .get_stream_config(stream_id)
            .await
            .map_err(|e| e.to_string())
    }
}
