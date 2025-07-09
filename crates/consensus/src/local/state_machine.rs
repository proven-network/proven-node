use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tracing::info;

/// Stream data for local consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    /// Messages in the stream
    pub messages: BTreeMap<u64, MessageData>,
    /// Last sequence number
    pub last_seq: u64,
    /// Whether the stream is paused for migration
    pub is_paused: bool,
    /// Pending operations while paused (for atomic resume)
    pub pending_operations: Vec<PendingOperation>,
    /// Pause timestamp for migration coordination
    pub paused_at: Option<u64>,
}

/// Pending operation while stream is paused
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOperation {
    /// Operation type
    pub operation_type: PendingOperationType,
    /// Data for the operation
    pub data: bytes::Bytes,
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

/// Message data in a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    /// Sequence number
    pub sequence: u64,
    /// Message content
    pub data: Bytes,
    /// Timestamp
    pub timestamp: u64,
    /// Optional metadata
    pub metadata: Option<HashMap<String, String>>,
}

/// Local state machine that manages streams assigned to this consensus group
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalState {
    /// Streams managed by this local consensus group
    streams: HashMap<String, StreamData>,
    /// Group ID for this store
    group_id: Option<crate::allocation::ConsensusGroupId>,
    /// Total messages across all streams
    total_messages: u64,
    /// Total bytes stored
    total_bytes: u64,
    /// Message rate tracking (messages per second over last minute)
    #[serde(skip)]
    rate_tracker: MessageRateTracker,
}

/// Track message rates over time
#[derive(Debug, Clone, Default)]
struct MessageRateTracker {
    /// Ring buffer of message counts per second (last 60 seconds)
    message_counts: Vec<u32>,
    /// Current second index in the ring buffer
    current_index: usize,
    /// Timestamp of the current second
    current_second: u64,
    /// Total messages in the last minute
    total_messages_per_minute: u64,
}

impl MessageRateTracker {
    fn new() -> Self {
        Self {
            message_counts: vec![0; 60],
            current_index: 0,
            current_second: 0,
            total_messages_per_minute: 0,
        }
    }

    fn record_message(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // If we've moved to a new second
        if now != self.current_second {
            // Calculate how many seconds have passed
            let seconds_passed = now.saturating_sub(self.current_second).min(60) as usize;

            // Clear the slots for the seconds that have passed
            for i in 1..=seconds_passed {
                let index = (self.current_index + i) % 60;
                self.total_messages_per_minute = self
                    .total_messages_per_minute
                    .saturating_sub(self.message_counts[index] as u64);
                self.message_counts[index] = 0;
            }

            // Move to the new second
            self.current_index = (self.current_index + seconds_passed) % 60;
            self.current_second = now;
        }

        // Increment the count for the current second
        self.message_counts[self.current_index] += 1;
        self.total_messages_per_minute += 1;
    }

    fn get_rate(&self) -> f64 {
        // Return messages per second (average over the last minute)
        self.total_messages_per_minute as f64 / 60.0
    }
}

impl LocalState {
    /// Create a new local state machine
    pub fn new(group_id: crate::allocation::ConsensusGroupId) -> Self {
        Self {
            streams: HashMap::new(),
            group_id: Some(group_id),
            total_messages: 0,
            total_bytes: 0,
            rate_tracker: MessageRateTracker::new(),
        }
    }

    /// Add a stream to this store (during allocation or migration)
    pub fn add_stream(&mut self, stream_name: String, data: StreamData) {
        let message_count = data.messages.len() as u64;
        let bytes = data
            .messages
            .values()
            .map(|m| m.data.len() as u64)
            .sum::<u64>();

        self.total_messages += message_count;
        self.total_bytes += bytes;

        self.streams.insert(stream_name, data);
    }

    /// Remove a stream from this store (during migration or deletion)
    pub fn remove_stream(&mut self, stream_name: &str) -> Option<StreamData> {
        if let Some(data) = self.streams.remove(stream_name) {
            let message_count = data.messages.len() as u64;
            let bytes = data
                .messages
                .values()
                .map(|m| m.data.len() as u64)
                .sum::<u64>();

            self.total_messages = self.total_messages.saturating_sub(message_count);
            self.total_bytes = self.total_bytes.saturating_sub(bytes);

            Some(data)
        } else {
            None
        }
    }

    /// Get a stream's data
    pub fn get_stream(&self, stream_name: &str) -> Option<&StreamData> {
        self.streams.get(stream_name)
    }

    /// Get a mutable reference to a stream's data
    pub fn get_stream_mut(&mut self, stream_name: &str) -> Option<&mut StreamData> {
        self.streams.get_mut(stream_name)
    }

    /// Publish a message to a stream
    pub fn publish_message(
        &mut self,
        stream_name: &str,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<u64, String> {
        // Check if stream is paused first
        if let Some(stream) = self.streams.get(stream_name) {
            if stream.is_paused {
                // Queue the operation without holding a mutable reference
                let pending_op = PendingOperation {
                    operation_type: PendingOperationType::Publish,
                    data: data.clone(),
                    metadata: metadata.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                if let Some(stream) = self.streams.get_mut(stream_name) {
                    stream.pending_operations.push(pending_op);
                    // Return the next sequence that would be assigned
                    return Ok(stream.last_seq + 1);
                }
            }
        }

        // Stream not found or not paused, proceed with normal operation
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream '{}' not found in this group", stream_name))?;

        let sequence = stream.last_seq + 1;
        stream.last_seq = sequence;

        let message = MessageData {
            sequence,
            data: data.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata,
        };

        let message_size = data.len() as u64;
        self.total_messages += 1;
        self.total_bytes += message_size;

        // Track message rate
        self.rate_tracker.record_message();

        stream.messages.insert(sequence, message);

        // Apply retention policies
        self.apply_retention(stream_name);

        Ok(sequence)
    }

    /// Apply retention policies to a stream
    fn apply_retention(&mut self, stream_name: &str) {
        // This is a simplified version - would need to check stream config
        if let Some(stream) = self.streams.get_mut(stream_name) {
            // Keep only last 10000 messages for now
            if stream.messages.len() > 10000 {
                let to_remove: Vec<_> = stream
                    .messages
                    .keys()
                    .take(stream.messages.len() - 10000)
                    .copied()
                    .collect();

                for seq in to_remove {
                    if let Some(msg) = stream.messages.remove(&seq) {
                        self.total_messages = self.total_messages.saturating_sub(1);
                        self.total_bytes = self.total_bytes.saturating_sub(msg.data.len() as u64);
                    }
                }
            }
        }
    }

    /// Get metrics for this store
    pub fn get_metrics(&self) -> LocalStateMetrics {
        LocalStateMetrics {
            group_id: self.group_id,
            stream_count: self.streams.len() as u32,
            total_messages: self.total_messages,
            total_bytes: self.total_bytes,
            message_rate: self.rate_tracker.get_rate(),
            streams: self
                .streams
                .iter()
                .map(|(name, data)| {
                    let bytes = data.messages.values().map(|m| m.data.len() as u64).sum();

                    (
                        name.clone(),
                        StreamMetrics {
                            message_count: data.messages.len() as u64,
                            last_sequence: data.last_seq,
                            total_bytes: bytes,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Get the current message rate (messages per second)
    pub fn get_message_rate(&self) -> f64 {
        self.rate_tracker.get_rate()
    }

    /// Create a snapshot of this store's state
    pub fn snapshot(&self) -> LocalStateSnapshot {
        LocalStateSnapshot {
            group_id: self.group_id,
            streams: self.streams.clone(),
            total_messages: self.total_messages,
            total_bytes: self.total_bytes,
        }
    }

    /// Restore from a snapshot
    pub fn restore_from_snapshot(&mut self, snapshot: LocalStateSnapshot) {
        self.group_id = snapshot.group_id;
        self.streams = snapshot.streams;
        self.total_messages = snapshot.total_messages;
        self.total_bytes = snapshot.total_bytes;
    }

    /// Create a stream for migration
    pub fn create_stream_for_migration(
        &mut self,
        stream_name: String,
        _source_group: crate::allocation::ConsensusGroupId,
    ) {
        self.add_stream(
            stream_name,
            StreamData {
                messages: BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );
    }

    /// Get checkpoint data for a stream
    pub fn get_stream_checkpoint(&self, stream_name: &str) -> Result<(StreamData, u64), String> {
        self.streams
            .get(stream_name)
            .map(|stream| (stream.clone(), stream.last_seq))
            .ok_or_else(|| format!("Stream {} not found", stream_name))
    }

    /// Create a comprehensive migration checkpoint for a stream
    pub fn create_migration_checkpoint(
        &self,
        stream_name: &str,
    ) -> Result<crate::migration::MigrationCheckpoint, String> {
        let stream_data = self
            .streams
            .get(stream_name)
            .cloned()
            .ok_or_else(|| format!("Stream {} not found", stream_name))?;

        // In a real implementation, we would:
        // 1. Get stream configuration from global state
        // 2. Get subscription list from subscription manager
        // 3. Calculate proper checksum

        let serialized_data = serde_json::to_vec(&stream_data)
            .map_err(|e| format!("Failed to serialize stream data: {}", e))?;

        let checksum = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&serialized_data);
            format!("{:x}", hasher.finalize())
        };

        Ok(crate::migration::MigrationCheckpoint {
            stream_name: stream_name.to_string(),
            sequence: stream_data.last_seq,
            data: stream_data,
            config: crate::global::StreamConfig::default(), // TODO: Get from global state
            subscriptions: Vec::new(),                      // TODO: Get from subscription manager
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum,
            compression: crate::migration::CompressionType::default(),
            is_incremental: false,
            base_checkpoint_seq: None,
        })
    }

    /// Create an incremental checkpoint containing only messages after a specific sequence
    pub fn create_incremental_checkpoint(
        &self,
        stream_name: &str,
        since_sequence: u64,
    ) -> Result<crate::migration::MigrationCheckpoint, String> {
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| format!("Stream {} not found", stream_name))?;

        // Create a new StreamData with only messages after since_sequence
        let incremental_messages: BTreeMap<u64, MessageData> = stream
            .messages
            .range((since_sequence + 1)..)
            .map(|(seq, data)| (*seq, data.clone()))
            .collect();

        let incremental_stream_data = StreamData {
            messages: incremental_messages.clone(),
            last_seq: stream.last_seq,
            is_paused: stream.is_paused,
            pending_operations: stream.pending_operations.clone(),
            paused_at: stream.paused_at,
        };

        // Calculate checksum for the incremental data
        let serialized_data = serde_json::to_vec(&incremental_stream_data)
            .map_err(|e| format!("Failed to serialize incremental data: {}", e))?;
        let checksum = crate::migration::calculate_checksum(&serialized_data);

        // Get message count for logging
        let message_count = incremental_messages.len();
        let sequence_range = if !incremental_messages.is_empty() {
            let first_seq = *incremental_messages.keys().next().unwrap();
            let last_seq = *incremental_messages.keys().last().unwrap();
            format!("{}-{}", first_seq, last_seq)
        } else {
            "empty".to_string()
        };

        tracing::info!(
            "Created incremental checkpoint for stream {} with {} messages (sequences: {})",
            stream_name,
            message_count,
            sequence_range
        );

        Ok(crate::migration::MigrationCheckpoint {
            stream_name: stream_name.to_string(),
            sequence: stream.last_seq,
            data: incremental_stream_data,
            config: crate::global::StreamConfig::default(), // TODO: Get from global state
            subscriptions: Vec::new(),                      // TODO: Get from subscription manager
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum,
            compression: crate::migration::CompressionType::default(),
            is_incremental: true,
            base_checkpoint_seq: Some(since_sequence),
        })
    }

    /// Apply a migration checkpoint
    pub fn apply_migration_checkpoint(&mut self, checkpoint_data: &[u8]) -> Result<(), String> {
        // Try to deserialize as compressed checkpoint first
        let checkpoint = if let Ok(compressed) =
            serde_json::from_slice::<crate::migration::CompressedCheckpoint>(checkpoint_data)
        {
            // Decompress the checkpoint
            crate::migration::decompress_checkpoint(&compressed)
                .map_err(|e| format!("Failed to decompress checkpoint: {}", e))?
        } else {
            // Fall back to uncompressed checkpoint
            serde_json::from_slice::<crate::migration::MigrationCheckpoint>(checkpoint_data)
                .map_err(|e| format!("Failed to deserialize checkpoint: {}", e))?
        };

        // Validate checkpoint integrity
        crate::migration::validate_checkpoint(&checkpoint)
            .map_err(|e| format!("Checkpoint validation failed: {}", e))?;

        // Add or update the stream with checkpoint data
        self.streams
            .insert(checkpoint.stream_name.clone(), checkpoint.data);

        // Update metrics
        self.recalculate_metrics();

        Ok(())
    }

    /// Apply an incremental migration checkpoint (merges with existing data)
    pub fn apply_incremental_checkpoint(&mut self, checkpoint_data: &[u8]) -> Result<(), String> {
        // Try to deserialize as compressed checkpoint first
        let checkpoint = if let Ok(compressed) =
            serde_json::from_slice::<crate::migration::CompressedCheckpoint>(checkpoint_data)
        {
            // Decompress the checkpoint
            crate::migration::decompress_checkpoint(&compressed)
                .map_err(|e| format!("Failed to decompress checkpoint: {}", e))?
        } else {
            // Fall back to uncompressed checkpoint
            serde_json::from_slice::<crate::migration::MigrationCheckpoint>(checkpoint_data)
                .map_err(|e| format!("Failed to deserialize checkpoint: {}", e))?
        };

        // Validate checkpoint integrity
        crate::migration::validate_checkpoint(&checkpoint)
            .map_err(|e| format!("Checkpoint validation failed: {}", e))?;

        // Verify this is an incremental checkpoint
        if !checkpoint.is_incremental {
            return Err("Expected incremental checkpoint but got full checkpoint".to_string());
        }

        // Get or create the stream
        let stream = self
            .streams
            .entry(checkpoint.stream_name.clone())
            .or_insert_with(|| StreamData {
                messages: BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            });

        // Merge incremental messages into existing stream
        let mut messages_added = 0;
        let mut bytes_added = 0u64;

        for (seq, message) in checkpoint.data.messages {
            // Only add messages that we don't already have
            if let std::collections::btree_map::Entry::Vacant(e) = stream.messages.entry(seq) {
                bytes_added += message.data.len() as u64;
                e.insert(message);
                messages_added += 1;
            }
        }

        // Update last_seq if the checkpoint has a higher sequence
        if checkpoint.data.last_seq > stream.last_seq {
            stream.last_seq = checkpoint.data.last_seq;
        }

        // Merge pause state and pending operations if needed
        if checkpoint.data.is_paused && !stream.is_paused {
            stream.is_paused = checkpoint.data.is_paused;
            stream.paused_at = checkpoint.data.paused_at;
        }

        // Append any new pending operations (avoiding duplicates)
        for pending_op in checkpoint.data.pending_operations {
            if !stream
                .pending_operations
                .iter()
                .any(|op| op.timestamp == pending_op.timestamp && op.data == pending_op.data)
            {
                stream.pending_operations.push(pending_op);
            }
        }

        // Update metrics
        self.total_messages += messages_added as u64;
        self.total_bytes += bytes_added;

        info!(
            "Applied incremental checkpoint for stream {}: {} new messages, {} bytes added",
            checkpoint.stream_name, messages_added, bytes_added
        );

        Ok(())
    }

    /// Atomically pause a stream for migration
    pub fn pause_stream(&mut self, stream_name: &str) -> Result<u64, String> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream {} not found", stream_name))?;

        if stream.is_paused {
            return Err(format!("Stream {} is already paused", stream_name));
        }

        let pause_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        stream.is_paused = true;
        stream.paused_at = Some(pause_timestamp);
        stream.pending_operations.clear(); // Clear any existing pending operations

        info!(
            "Stream {} paused for migration at timestamp {}",
            stream_name, pause_timestamp
        );
        Ok(stream.last_seq)
    }

    /// Atomically resume a stream and apply pending operations
    pub fn resume_stream(&mut self, stream_name: &str) -> Result<Vec<u64>, String> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream {} not found", stream_name))?;

        if !stream.is_paused {
            return Err(format!("Stream {} is not paused", stream_name));
        }

        // Apply pending operations atomically
        let mut applied_sequences = Vec::new();
        for pending_op in &stream.pending_operations {
            match &pending_op.operation_type {
                PendingOperationType::Publish => {
                    let sequence = stream.last_seq + 1;
                    stream.last_seq = sequence;

                    let message = MessageData {
                        sequence,
                        data: pending_op.data.clone(),
                        timestamp: pending_op.timestamp,
                        metadata: pending_op.metadata.clone(),
                    };

                    stream.messages.insert(sequence, message);
                    applied_sequences.push(sequence);

                    // Update metrics
                    self.total_messages += 1;
                    self.total_bytes += pending_op.data.len() as u64;
                }
                PendingOperationType::Rollup => {
                    // Handle rollup operation
                    let sequence = stream.last_seq + 1;
                    stream.last_seq = sequence;

                    let message = MessageData {
                        sequence,
                        data: pending_op.data.clone(),
                        timestamp: pending_op.timestamp,
                        metadata: pending_op.metadata.clone(),
                    };

                    stream.messages.insert(sequence, message);
                    applied_sequences.push(sequence);

                    self.total_messages += 1;
                    self.total_bytes += pending_op.data.len() as u64;
                }
                PendingOperationType::Delete { sequence } => {
                    if let Some(removed_msg) = stream.messages.remove(sequence) {
                        self.total_messages = self.total_messages.saturating_sub(1);
                        self.total_bytes = self
                            .total_bytes
                            .saturating_sub(removed_msg.data.len() as u64);
                        applied_sequences.push(*sequence);
                    }
                }
            }
        }

        // Clear pause state
        stream.is_paused = false;
        stream.paused_at = None;
        stream.pending_operations.clear();

        info!(
            "Stream {} resumed, applied {} pending operations",
            stream_name,
            applied_sequences.len()
        );

        Ok(applied_sequences)
    }

    /// Check if a stream is paused
    pub fn is_stream_paused(&self, stream_name: &str) -> bool {
        self.streams
            .get(stream_name)
            .map(|stream| stream.is_paused)
            .unwrap_or(false)
    }

    /// Queue an operation while stream is paused
    pub fn queue_pending_operation(
        &mut self,
        stream_name: &str,
        operation_type: PendingOperationType,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), String> {
        let stream = self
            .streams
            .get_mut(stream_name)
            .ok_or_else(|| format!("Stream {} not found", stream_name))?;

        if !stream.is_paused {
            return Err(format!(
                "Stream {} is not paused, cannot queue operations",
                stream_name
            ));
        }

        let pending_op = PendingOperation {
            operation_type,
            data,
            metadata,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        stream.pending_operations.push(pending_op);
        Ok(())
    }

    /// Remove a stream for migration
    pub fn remove_stream_for_migration(&mut self, stream_name: &str) -> Result<(), String> {
        if let Some(stream) = self.streams.remove(stream_name) {
            // Update metrics
            self.total_messages = self
                .total_messages
                .saturating_sub(stream.messages.len() as u64);
            let bytes: u64 = stream.messages.values().map(|m| m.data.len() as u64).sum();
            self.total_bytes = self.total_bytes.saturating_sub(bytes);
            Ok(())
        } else {
            Err(format!("Stream {} not found", stream_name))
        }
    }

    /// Recalculate metrics after changes
    fn recalculate_metrics(&mut self) {
        self.total_messages = 0;
        self.total_bytes = 0;

        for stream in self.streams.values() {
            self.total_messages += stream.messages.len() as u64;
            self.total_bytes += stream
                .messages
                .values()
                .map(|m| m.data.len() as u64)
                .sum::<u64>();
        }
    }

    /// Clean up old pending operations that have been sitting for too long
    pub fn cleanup_old_pending_operations(&mut self, max_age_secs: u64) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut total_cleaned = 0;

        for stream in self.streams.values_mut() {
            if stream.is_paused {
                let before_count = stream.pending_operations.len();

                // Remove operations older than max_age_secs
                stream
                    .pending_operations
                    .retain(|op| now.saturating_sub(op.timestamp) <= max_age_secs);

                let cleaned = before_count - stream.pending_operations.len();
                if cleaned > 0 {
                    info!(
                        "Cleaned {} old pending operations from paused stream",
                        cleaned
                    );
                    total_cleaned += cleaned;
                }
            }
        }

        total_cleaned
    }

    /// Get the count of pending operations across all streams
    pub fn get_pending_operations_count(&self) -> usize {
        self.streams
            .values()
            .map(|stream| stream.pending_operations.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_local_global_state() {
        let group_id = crate::allocation::ConsensusGroupId::new(1);
        let mut store = LocalState::new(group_id);

        // Add a stream
        let stream_data = StreamData {
            messages: BTreeMap::new(),
            last_seq: 0,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };
        store.add_stream("test-stream".to_string(), stream_data);

        // Publish a message
        let seq = store
            .publish_message("test-stream", Bytes::from("hello world"), None)
            .unwrap();

        assert_eq!(seq, 1);

        // Verify message was stored
        let stream = store.get_stream("test-stream").unwrap();
        assert_eq!(stream.messages.len(), 1);
        assert_eq!(stream.last_seq, 1);

        // Test metrics
        let metrics = store.get_metrics();
        assert_eq!(metrics.stream_count, 1);
        assert_eq!(metrics.total_messages, 1);
    }

    #[tokio::test]
    async fn test_migration_checkpoint() {
        let group_id = crate::allocation::ConsensusGroupId::new(0);
        let mut store = LocalState::new(group_id);

        // Create a stream with some data
        let stream_name = "test-stream";
        let mut messages = BTreeMap::new();
        messages.insert(
            1,
            MessageData {
                sequence: 1,
                data: Bytes::from("message 1"),
                timestamp: 1000,
                metadata: None,
            },
        );
        messages.insert(
            2,
            MessageData {
                sequence: 2,
                data: Bytes::from("message 2"),
                timestamp: 2000,
                metadata: None,
            },
        );

        store.add_stream(
            stream_name.to_string(),
            StreamData {
                messages,
                last_seq: 2,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );

        // Get checkpoint
        let (stream_data, last_seq) = store.get_stream_checkpoint(stream_name).unwrap();
        assert_eq!(last_seq, 2);
        assert_eq!(stream_data.messages.len(), 2);

        // Create checkpoint for migration using the proper method
        let checkpoint = store.create_migration_checkpoint(stream_name).unwrap();

        // Simulate applying checkpoint to a new store
        let mut target_store = LocalState::new(crate::allocation::ConsensusGroupId::new(1));
        let checkpoint_data = serde_json::to_vec(&checkpoint).unwrap();
        target_store
            .apply_migration_checkpoint(&checkpoint_data)
            .unwrap();

        // Verify stream was migrated
        let migrated_stream = target_store.get_stream(stream_name).unwrap();
        assert_eq!(migrated_stream.last_seq, 2);
        assert_eq!(migrated_stream.messages.len(), 2);
    }

    #[tokio::test]
    async fn test_migration_with_ongoing_writes() {
        let group_id = crate::allocation::ConsensusGroupId::new(0);
        let mut store = LocalState::new(group_id);

        let stream_name = "active-stream";
        store.add_stream(
            stream_name.to_string(),
            StreamData {
                messages: BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );

        // Publish some initial messages
        for i in 1..=5 {
            let seq = store
                .publish_message(stream_name, Bytes::from(format!("message {}", i)), None)
                .unwrap();
            assert_eq!(seq, i);
        }

        // Pause the stream (simulating migration)
        store.pause_stream(stream_name).unwrap();

        // Get checkpoint
        let (_stream_data, last_seq) = store.get_stream_checkpoint(stream_name).unwrap();
        assert_eq!(last_seq, 5);

        // Resume the stream
        store.resume_stream(stream_name).unwrap();

        // Continue publishing
        let seq = store
            .publish_message(stream_name, Bytes::from("message after resume"), None)
            .unwrap();
        assert_eq!(seq, 6);
    }

    #[test]
    fn test_message_rate_tracking() {
        let group_id = crate::allocation::ConsensusGroupId::new(0);
        let mut store = LocalState::new(group_id);

        // Add a stream
        store.add_stream(
            "test-stream".to_string(),
            StreamData {
                messages: BTreeMap::new(),
                last_seq: 0,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );

        // Initial rate should be 0
        assert_eq!(store.get_message_rate(), 0.0);

        // Publish messages
        for i in 1..=10 {
            store
                .publish_message("test-stream", Bytes::from(format!("msg {}", i)), None)
                .unwrap();
        }

        // Rate should be positive now
        let rate = store.get_message_rate();
        assert!(rate > 0.0);

        // Check metrics include rate
        let metrics = store.get_metrics();
        assert_eq!(metrics.message_rate, rate);
    }

    #[test]
    fn test_pending_operations_cleanup() {
        let group_id = crate::allocation::ConsensusGroupId::new(0);
        let mut store = LocalState::new(group_id);

        // Add a stream
        store.add_stream(
            "test-stream".to_string(),
            StreamData {
                messages: BTreeMap::new(),
                last_seq: 0,
                is_paused: true, // Already paused
                pending_operations: vec![
                    PendingOperation {
                        operation_type: PendingOperationType::Publish,
                        data: Bytes::from("old operation"),
                        metadata: None,
                        timestamp: 1000, // Very old timestamp
                    },
                    PendingOperation {
                        operation_type: PendingOperationType::Publish,
                        data: Bytes::from("recent operation"),
                        metadata: None,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            - 10, // 10 seconds ago
                    },
                ],
                paused_at: Some(1000),
            },
        );

        // Should have 2 pending operations initially
        assert_eq!(store.get_pending_operations_count(), 2);

        // Clean up operations older than 60 seconds
        let cleaned = store.cleanup_old_pending_operations(60);
        assert_eq!(cleaned, 1); // Should clean the old one

        // Should have 1 pending operation left
        assert_eq!(store.get_pending_operations_count(), 1);
    }

    #[test]
    fn test_incremental_checkpoint_creation() {
        let group_id = crate::allocation::ConsensusGroupId::new(0);
        let mut store = LocalState::new(group_id);

        // Create a stream with messages
        let stream_name = "test-stream";
        let mut messages = BTreeMap::new();
        for i in 1..=20 {
            messages.insert(
                i,
                MessageData {
                    sequence: i,
                    data: Bytes::from(format!("message {}", i)),
                    timestamp: 1000 + i,
                    metadata: None,
                },
            );
        }

        store.add_stream(
            stream_name.to_string(),
            StreamData {
                messages,
                last_seq: 20,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
        );

        // Create incremental checkpoint from sequence 10
        let checkpoint = store
            .create_incremental_checkpoint(stream_name, 10)
            .unwrap();

        // Should only contain messages 11-20
        assert_eq!(checkpoint.data.messages.len(), 10);
        assert!(checkpoint.is_incremental);
        assert_eq!(checkpoint.base_checkpoint_seq, Some(10));

        // Verify message sequences
        let sequences: Vec<u64> = checkpoint.data.messages.keys().copied().collect();
        assert_eq!(sequences, vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20]);
    }
}

/// Metrics for a local state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStateMetrics {
    /// Group ID
    pub group_id: Option<crate::allocation::ConsensusGroupId>,
    /// Number of streams
    pub stream_count: u32,
    /// Total messages
    pub total_messages: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Current message rate (messages per second)
    pub message_rate: f64,
    /// Per-stream metrics
    pub streams: HashMap<String, StreamMetrics>,
}

/// Metrics for a single stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetrics {
    /// Number of messages
    pub message_count: u64,
    /// Last sequence number
    pub last_sequence: u64,
    /// Total bytes
    pub total_bytes: u64,
}

/// Snapshot of a local state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStateSnapshot {
    /// Group ID
    pub group_id: Option<crate::allocation::ConsensusGroupId>,
    /// All streams and their data
    pub streams: HashMap<String, StreamData>,
    /// Total messages
    pub total_messages: u64,
    /// Total bytes
    pub total_bytes: u64,
}
