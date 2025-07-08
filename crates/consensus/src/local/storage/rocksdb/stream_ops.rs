//! Stream storage operations implementation for RocksDB

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use rocksdb::{Direction, IteratorMode, WriteBatch};
use tracing::{debug, info, warn};

use crate::error::{ConsensusResult, Error};
use crate::global::StreamConfig;
use crate::migration::MigrationCheckpoint;

use super::storage::{CF_STREAM_INDEX, KEY_PREFIX_META, KEY_PREFIX_PAUSE, LocalRocksDBStorage};
use crate::local::storage::traits::{
    CheckpointFormat, CompressionType, MessageData, PauseReason, PauseState, StreamExport,
    StreamMetadata, StreamMetrics, StreamStorage,
};

#[async_trait]
impl StreamStorage for LocalRocksDBStorage {
    async fn export_stream(&self, stream_name: &str) -> ConsensusResult<StreamExport> {
        info!("Exporting stream: {}", stream_name);

        // Get stream metadata
        let metadata = self
            .get_stream_metadata(stream_name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("Stream not found: {}", stream_name)))?;

        // Get stream column family
        let cf = self.get_stream_cf(stream_name).await?;

        // Collect all messages
        let mut messages = Vec::new();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (key, value) =
                item.map_err(|e| Error::Storage(format!("Iterator error: {}", e)))?;

            let seq = Self::decode_sequence(&key)?;
            let msg_data: MessageData = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize message: {}", e)))?;

            messages.push((seq, msg_data));
        }

        Ok(StreamExport {
            metadata,
            messages,
            checkpoint_format: CheckpointFormat::Full,
            compression: CompressionType::None,
            exported_at: chrono::Utc::now().timestamp_millis() as u64,
            source_group: self.group_id,
        })
    }

    async fn import_stream(&self, export: StreamExport) -> ConsensusResult<()> {
        info!("Importing stream: {}", export.metadata.name);

        let stream_name = &export.metadata.name;

        // Check if stream already exists
        if self.stream_exists(stream_name).await? {
            return Err(Error::AlreadyExists(format!(
                "Stream already exists: {}",
                stream_name
            )));
        }

        // Create stream column family
        let cf = self.get_or_create_stream_cf(stream_name).await?;

        // Use batch for atomic import
        let mut batch = WriteBatch::default();

        // Import all messages
        for (seq, msg_data) in export.messages {
            let key = Self::encode_sequence(seq);
            let value = serde_json::to_vec(&msg_data)
                .map_err(|e| Error::Storage(format!("Failed to serialize message: {}", e)))?;

            batch.put_cf(&cf, key, value);
        }

        // Update metadata
        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let meta_key = format!("{}{}", KEY_PREFIX_META, stream_name);
        let meta_value = serde_json::to_vec(&export.metadata)
            .map_err(|e| Error::Storage(format!("Failed to serialize metadata: {}", e)))?;

        batch.put_cf(&cf_index, meta_key.as_bytes(), meta_value);

        // Write batch atomically
        self.db
            .write(batch)
            .map_err(|e| Error::Storage(format!("Failed to import stream: {}", e)))?;

        // Update in-memory state machine
        let mut state_machine = self.state_machine.write().await;
        // Create empty stream data
        let stream_data = crate::local::state_machine::StreamData {
            messages: std::collections::BTreeMap::new(),
            last_seq: export.metadata.last_sequence,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };
        state_machine.add_stream(stream_name.to_string(), stream_data);

        info!("Successfully imported stream: {}", stream_name);
        Ok(())
    }

    async fn create_checkpoint(
        &self,
        stream_name: &str,
        since_seq: Option<u64>,
    ) -> ConsensusResult<MigrationCheckpoint> {
        debug!(
            "Creating checkpoint for stream: {} (since_seq: {:?})",
            stream_name, since_seq
        );

        // Get stream metadata
        let metadata = self
            .get_stream_metadata(stream_name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("Stream not found: {}", stream_name)))?;

        // Get stream column family
        let cf = self.get_stream_cf(stream_name).await?;

        // Determine checkpoint type
        let (_checkpoint_format, start_seq) = if let Some(base_seq) = since_seq {
            (CheckpointFormat::Incremental { base_seq }, base_seq + 1)
        } else {
            (CheckpointFormat::Full, 0)
        };

        // Collect messages
        let mut messages = BTreeMap::new();
        let start_key = Self::encode_sequence(start_seq);
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        let mut total_size = 0;
        const MAX_CHECKPOINT_SIZE: usize = 100 * 1024 * 1024; // 100MB limit

        for item in iter {
            let (key, value) =
                item.map_err(|e| Error::Storage(format!("Iterator error: {}", e)))?;

            let seq = Self::decode_sequence(&key)?;

            // Convert to state machine format
            let msg_data: MessageData = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize message: {}", e)))?;

            let state_msg = crate::local::state_machine::MessageData {
                data: msg_data.data.clone(),
                metadata: msg_data.metadata.clone(),
                timestamp: msg_data.timestamp,
                sequence: seq,
            };

            messages.insert(seq, state_msg);
            total_size += value.len();

            // Limit checkpoint size
            if total_size > MAX_CHECKPOINT_SIZE {
                warn!(
                    "Checkpoint size limit reached for stream: {} (collected {} messages)",
                    stream_name,
                    messages.len()
                );
                break;
            }
        }

        // Check pause state
        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let pause_key = format!("{}{}", KEY_PREFIX_PAUSE, stream_name);
        let is_paused = self
            .db
            .get_cf(&cf_index, pause_key.as_bytes())
            .map_err(|e| Error::Storage(format!("Failed to check pause state: {}", e)))?
            .is_some();

        // Get stream data for checkpoint
        let stream_data = crate::local::state_machine::StreamData {
            messages,
            last_seq: metadata.last_sequence,
            is_paused,
            pending_operations: Vec::new(), // TODO: Load from storage
            paused_at: if is_paused {
                Some(chrono::Utc::now().timestamp_millis() as u64)
            } else {
                None
            },
        };

        // Calculate checksum
        let serialized_data = serde_json::to_vec(&stream_data)
            .map_err(|e| Error::Storage(format!("Failed to serialize for checksum: {}", e)))?;
        let checksum = crate::migration::calculate_checksum(&serialized_data);

        // Create checkpoint
        let checkpoint = MigrationCheckpoint {
            stream_name: stream_name.to_string(),
            sequence: metadata.last_sequence,
            data: stream_data,
            checksum,
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            config: StreamConfig {
                max_messages: metadata.config.max_messages,
                max_bytes: Some(1024 * 1024 * 1024), // 1GB default
                max_age_secs: metadata.config.retention_seconds,
                storage_type: crate::global::StorageType::Memory, // Default
                retention_policy: crate::global::RetentionPolicy::Limits,
                pubsub_bridge_enabled: false,
                consensus_group: Some(self.group_id),
            },
            subscriptions: Vec::new(), // TODO: Load subscriptions
            compression: crate::migration::CompressionType::None,
            is_incremental: since_seq.is_some(),
            base_checkpoint_seq: since_seq,
        };

        Ok(checkpoint)
    }

    async fn apply_checkpoint(&self, checkpoint: &MigrationCheckpoint) -> ConsensusResult<()> {
        info!("Applying checkpoint for stream: {}", checkpoint.stream_name);

        let stream_name = &checkpoint.stream_name;

        // Create stream if it doesn't exist
        let cf = self.get_or_create_stream_cf(stream_name).await?;

        // Use batch for atomic application
        let mut batch = WriteBatch::default();

        // Apply all messages
        for (seq, msg_data) in &checkpoint.data.messages {
            let key = Self::encode_sequence(*seq);

            // Convert from state machine format to storage format
            let storage_msg = MessageData {
                data: msg_data.data.clone(),
                metadata: msg_data.metadata.clone(),
                timestamp: msg_data.timestamp,
            };

            let value = serde_json::to_vec(&storage_msg)
                .map_err(|e| Error::Storage(format!("Failed to serialize message: {}", e)))?;

            batch.put_cf(&cf, key, value);
        }

        // Update metadata
        let metadata = StreamMetadata {
            name: stream_name.to_string(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            modified_at: chrono::Utc::now().timestamp_millis() as u64,
            last_sequence: checkpoint.sequence,
            message_count: checkpoint.data.messages.len() as u64,
            config: crate::local::storage::traits::StreamConfig {
                retention_seconds: checkpoint.config.max_age_secs,
                max_messages: checkpoint.config.max_messages,
                compact_on_deletion: true,
                compression: CompressionType::Lz4,
            },
            is_paused: checkpoint.data.is_paused,
            custom_metadata: HashMap::new(),
        };

        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let meta_key = format!("{}{}", KEY_PREFIX_META, stream_name);
        let meta_value = serde_json::to_vec(&metadata)
            .map_err(|e| Error::Storage(format!("Failed to serialize metadata: {}", e)))?;

        batch.put_cf(&cf_index, meta_key.as_bytes(), meta_value);

        // Apply pause state if needed
        if checkpoint.data.is_paused {
            let pause_state = PauseState {
                paused_at: checkpoint.data.paused_at.unwrap_or(checkpoint.created_at),
                last_seq: checkpoint.sequence,
                reason: PauseReason::Migration {
                    target_group: self.group_id, // TODO: Get from migration context
                },
            };

            let pause_key = format!("{}{}", KEY_PREFIX_PAUSE, stream_name);
            let pause_value = serde_json::to_vec(&pause_state)
                .map_err(|e| Error::Storage(format!("Failed to serialize pause state: {}", e)))?;

            batch.put_cf(&cf_index, pause_key.as_bytes(), pause_value);
        }

        // Write batch atomically
        self.db
            .write(batch)
            .map_err(|e| Error::Storage(format!("Failed to apply checkpoint: {}", e)))?;

        info!(
            "Successfully applied checkpoint for stream: {}",
            stream_name
        );
        Ok(())
    }

    async fn get_stream_metrics(&self, stream_name: &str) -> ConsensusResult<StreamMetrics> {
        // Get metadata
        let metadata = self
            .get_stream_metadata(stream_name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("Stream not found: {}", stream_name)))?;

        // Get column family
        let cf = self.get_stream_cf(stream_name).await?;

        // Estimate disk usage using RocksDB properties
        let disk_usage = self
            .db
            .property_int_value_cf(&cf, "rocksdb.estimate-live-data-size")
            .map_err(|e| Error::Storage(format!("Failed to get disk usage: {}", e)))?
            .unwrap_or(0);

        // Get timestamp range
        let mut oldest_timestamp = None;
        let mut newest_timestamp = None;

        // Check first message
        if let Some(Ok((_, value))) = self.db.iterator_cf(&cf, IteratorMode::Start).next() {
            let msg: MessageData = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize message: {}", e)))?;
            oldest_timestamp = Some(msg.timestamp);
        }

        // Check last message
        if let Some(Ok((_, value))) = self.db.iterator_cf(&cf, IteratorMode::End).next() {
            let msg: MessageData = serde_json::from_slice(&value)
                .map_err(|e| Error::Storage(format!("Failed to deserialize message: {}", e)))?;
            newest_timestamp = Some(msg.timestamp);
        }

        Ok(StreamMetrics {
            stream_name: stream_name.to_string(),
            disk_usage_bytes: disk_usage,
            message_count: metadata.message_count,
            oldest_message_timestamp: oldest_timestamp,
            newest_message_timestamp: newest_timestamp,
            write_rate: 0.0, // TODO: Implement rate tracking
            read_rate: 0.0,  // TODO: Implement rate tracking
        })
    }

    async fn stream_exists(&self, stream_name: &str) -> ConsensusResult<bool> {
        Ok(self.get_stream_metadata(stream_name).await?.is_some())
    }

    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<()> {
        info!("Deleting stream: {}", stream_name);

        let cf_name = format!("stream_{}", stream_name);

        // Drop the column family (efficient in RocksDB)
        // Use the same workaround as create_cf
        let db_clone = Arc::clone(&self.db);
        let db_ptr = Arc::as_ptr(&db_clone) as *mut rocksdb::DB;
        unsafe {
            (*db_ptr)
                .drop_cf(&cf_name)
                .map_err(|e| Error::Storage(format!("Failed to drop column family: {}", e)))?;
        }

        // Column family has been dropped from the database

        // Remove metadata and other index entries
        let cf_index = self.get_cf(CF_STREAM_INDEX).await?;
        let mut batch = WriteBatch::default();

        // Delete metadata
        let meta_key = format!("{}{}", KEY_PREFIX_META, stream_name);
        batch.delete_cf(&cf_index, meta_key.as_bytes());

        // Delete pause state if exists
        let pause_key = format!("{}{}", KEY_PREFIX_PAUSE, stream_name);
        batch.delete_cf(&cf_index, pause_key.as_bytes());

        // Delete any pending operations
        // TODO: Implement pending operations cleanup

        self.db
            .write(batch)
            .map_err(|e| Error::Storage(format!("Failed to delete stream metadata: {}", e)))?;

        info!("Successfully deleted stream: {}", stream_name);
        Ok(())
    }
}
