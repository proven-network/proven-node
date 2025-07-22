//! Group consensus state
//!
//! Pure state container for group consensus operations.

use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_logger::error;

use super::types::MessageData;
use crate::services::stream::StreamName;

/// Group consensus state
#[derive(Clone)]
pub struct GroupState {
    /// Stream states
    streams: Arc<RwLock<HashMap<StreamName, StreamState>>>,

    /// Group metadata
    metadata: Arc<RwLock<GroupMetadata>>,
}

/// State for a single stream
#[derive(Debug, Clone)]
pub struct StreamState {
    /// Stream name
    pub name: StreamName,
    /// Next sequence number
    pub next_sequence: NonZero<u64>,
    /// First sequence (for trimmed streams)
    pub first_sequence: NonZero<u64>,
    /// Stream statistics
    pub stats: StreamStats,
}

/// Stream statistics
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total messages
    pub message_count: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Last update timestamp
    pub last_update: u64,
}

/// Group metadata
#[derive(Debug, Clone)]
pub struct GroupMetadata {
    /// Group creation time
    pub created_at: u64,
    /// Number of streams
    pub stream_count: usize,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total storage used
    pub total_bytes: u64,
}

impl GroupState {
    /// Create new group state
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(GroupMetadata {
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                stream_count: 0,
                total_messages: 0,
                total_bytes: 0,
            })),
        }
    }

    // Stream operations

    /// Initialize a stream
    pub async fn initialize_stream(&self, name: StreamName) -> bool {
        let mut streams = self.streams.write().await;

        if streams.contains_key(&name) {
            return false;
        }

        streams.insert(
            name.clone(),
            StreamState {
                name,
                next_sequence: NonZero::new(1).unwrap(),
                first_sequence: NonZero::new(1).unwrap(),
                stats: StreamStats::default(),
            },
        );

        // Update metadata
        let mut metadata = self.metadata.write().await;
        metadata.stream_count = streams.len();

        true
    }

    /// Remove a stream
    pub async fn remove_stream(&self, name: &StreamName) -> bool {
        let mut streams = self.streams.write().await;

        if let Some(state) = streams.remove(name) {
            // Update metadata
            let mut metadata = self.metadata.write().await;
            metadata.stream_count = streams.len();
            metadata.total_messages -= state.stats.message_count;
            metadata.total_bytes -= state.stats.total_bytes;

            true
        } else {
            false
        }
    }

    /// Append messages to stream and return pre-serialized entries
    pub async fn append_messages(
        &self,
        stream: &StreamName,
        messages: Vec<MessageData>,
        timestamp_millis: u64,
    ) -> Arc<Vec<bytes::Bytes>> {
        if messages.is_empty() {
            return Arc::new(vec![]);
        }

        let mut streams = self.streams.write().await;

        if let Some(state) = streams.get_mut(stream) {
            let mut entries = Vec::with_capacity(messages.len());
            let start_sequence = state.next_sequence;

            let mut total_size = 0u64;
            let mut message_count = 0u64;

            // Serialize each message to binary format
            for (i, message) in messages.into_iter().enumerate() {
                let sequence = start_sequence.saturating_add(i as u64).get();

                // Serialize to binary format
                match crate::foundation::serialize_entry(&message, timestamp_millis, sequence) {
                    Ok(serialized) => {
                        total_size += message.payload.len() as u64;
                        entries.push(serialized);
                        message_count += 1;
                    }
                    Err(e) => {
                        error!("Failed to serialize message: {e}");
                        // Skip this message
                        continue;
                    }
                }
            }

            // Update state
            state.next_sequence = start_sequence.saturating_add(message_count);
            state.stats.message_count += message_count;
            state.stats.total_bytes += total_size;
            state.stats.last_update = timestamp_millis / 1000; // Convert to seconds

            // Update metadata
            drop(streams);
            let mut metadata = self.metadata.write().await;
            metadata.total_messages += message_count;
            metadata.total_bytes += total_size;

            Arc::new(entries)
        } else {
            // Stream doesn't exist - this shouldn't happen as we validate in operations
            error!("Stream {stream} not found in state");
            Arc::new(vec![])
        }
    }

    /// Trim stream up to sequence
    pub async fn trim_stream(
        &self,
        stream: &StreamName,
        up_to_seq: NonZero<u64>,
    ) -> Option<NonZero<u64>> {
        let mut streams = self.streams.write().await;

        if let Some(state) = streams.get_mut(stream) {
            // Can only trim if up_to_seq is valid
            if up_to_seq >= state.first_sequence && up_to_seq < state.next_sequence {
                // Update first sequence
                state.first_sequence = up_to_seq.saturating_add(1);

                // Note: We can't update stats accurately without knowing the actual messages trimmed
                // This would need to be coordinated with StreamService
                state.stats.last_update = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                Some(state.first_sequence)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Delete a specific message from stream
    pub async fn delete_message(
        &self,
        stream: &StreamName,
        sequence: NonZero<u64>,
    ) -> Option<NonZero<u64>> {
        let streams = self.streams.read().await;

        if let Some(state) = streams.get(stream) {
            // Validate sequence is valid
            if sequence >= state.next_sequence {
                return None;
            }

            // Return the sequence to indicate success
            // The actual deletion happens in StreamService
            Some(sequence)
        } else {
            None
        }
    }

    // Query operations

    /// Get stream state
    pub async fn get_stream(&self, name: &StreamName) -> Option<StreamState> {
        let streams = self.streams.read().await;
        streams.get(name).cloned()
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamName> {
        let streams = self.streams.read().await;
        streams.keys().cloned().collect()
    }

    /// Get group metadata
    pub async fn get_metadata(&self) -> GroupMetadata {
        self.metadata.read().await.clone()
    }

    /// Get stream count
    pub async fn stream_count(&self) -> usize {
        self.streams.read().await.len()
    }

    /// Get total message count
    pub async fn total_messages(&self) -> u64 {
        self.metadata.read().await.total_messages
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::new()
    }
}
