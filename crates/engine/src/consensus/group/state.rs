//! Group consensus state
//!
//! Pure state container for group consensus operations.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::types::{MessageData, StoredMessage};
use crate::stream::StreamName;

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
    /// Messages stored in order
    pub messages: BTreeMap<u64, StoredMessage>,
    /// Next sequence number
    pub next_sequence: u64,
    /// First sequence (for trimmed streams)
    pub first_sequence: u64,
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
                messages: BTreeMap::new(),
                next_sequence: 1,
                first_sequence: 1,
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

    /// Append message to stream
    pub async fn append_message(&self, stream: &StreamName, message: MessageData) -> Option<u64> {
        let mut streams = self.streams.write().await;

        if let Some(state) = streams.get_mut(stream) {
            let sequence = state.next_sequence;
            state.next_sequence += 1;

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let message_size = message.payload.len() as u64;

            state.messages.insert(
                sequence,
                StoredMessage {
                    sequence,
                    data: message,
                    timestamp,
                },
            );

            // Update stats
            state.stats.message_count += 1;
            state.stats.total_bytes += message_size;
            state.stats.last_update = timestamp;

            // Update metadata
            drop(streams);
            let mut metadata = self.metadata.write().await;
            metadata.total_messages += 1;
            metadata.total_bytes += message_size;

            Some(sequence)
        } else {
            None
        }
    }

    /// Read messages from stream
    pub async fn read_messages(
        &self,
        stream: &StreamName,
        from_seq: u64,
        count: u32,
    ) -> Vec<StoredMessage> {
        let streams = self.streams.read().await;

        if let Some(state) = streams.get(stream) {
            state
                .messages
                .range(from_seq..)
                .take(count as usize)
                .map(|(_, msg)| msg.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Trim stream up to sequence
    pub async fn trim_stream(&self, stream: &StreamName, up_to_seq: u64) -> Option<u64> {
        let mut streams = self.streams.write().await;

        if let Some(state) = streams.get_mut(stream) {
            // Remove messages up to the sequence
            let removed: Vec<_> = state
                .messages
                .range(..=up_to_seq)
                .map(|(seq, _)| *seq)
                .collect();

            let mut removed_bytes = 0u64;
            let mut removed_count = 0u64;
            for seq in removed {
                if let Some(msg) = state.messages.remove(&seq) {
                    removed_bytes += msg.data.payload.len() as u64;
                    removed_count += 1;
                }
            }

            // Update first sequence
            if let Some((&first_seq, _)) = state.messages.iter().next() {
                state.first_sequence = first_seq;
            } else {
                state.first_sequence = state.next_sequence;
            }

            // Update stats
            state.stats.message_count -= removed_count;
            state.stats.total_bytes -= removed_bytes;
            state.stats.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let first_sequence = state.first_sequence;

            // Update metadata
            drop(streams);
            let mut metadata = self.metadata.write().await;
            metadata.total_messages -= removed_count;
            metadata.total_bytes -= removed_bytes;

            Some(first_sequence)
        } else {
            None
        }
    }

    /// Delete a specific message from stream
    pub async fn delete_message(&self, stream: &StreamName, sequence: u64) -> Option<u64> {
        let mut streams = self.streams.write().await;

        if let Some(state) = streams.get_mut(stream) {
            // Validate sequence is less than last seq and not 0
            if sequence == 0 || sequence >= state.next_sequence - 1 {
                return None;
            }

            // Remove the message
            if let Some(msg) = state.messages.remove(&sequence) {
                let removed_bytes = msg.data.payload.len() as u64;

                // Update stats
                state.stats.message_count -= 1;
                state.stats.total_bytes -= removed_bytes;
                state.stats.last_update = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                // Update metadata
                drop(streams);
                let mut metadata = self.metadata.write().await;
                metadata.total_messages -= 1;
                metadata.total_bytes -= removed_bytes;

                Some(sequence)
            } else {
                None
            }
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
