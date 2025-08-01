//! Group consensus state
//!
//! Pure state container for group consensus operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use proven_storage::LogIndex;

use crate::foundation::StreamName;
use crate::foundation::messages::Message;
use crate::foundation::models::{GroupMetadata, StreamState, StreamStats};

/// Group consensus state
#[derive(Clone)]
pub struct GroupState {
    /// Group stream states
    group_stream_states: Arc<DashMap<StreamName, StreamState>>,

    /// Metadata for group streams - using atomics for lock-free access
    created_at: Arc<AtomicU64>,
    stream_count: Arc<AtomicUsize>,
    total_messages: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
}

impl GroupState {
    /// Create new group state
    pub fn new() -> Self {
        Self {
            group_stream_states: Arc::new(DashMap::new()),
            created_at: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            )),
            stream_count: Arc::new(AtomicUsize::new(0)),
            total_messages: Arc::new(AtomicU64::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    // Stream operations

    /// Initialize a stream
    pub async fn initialize_stream(&self, name: StreamName) -> bool {
        if self.group_stream_states.contains_key(&name) {
            return false;
        }

        self.group_stream_states.insert(
            name.clone(),
            StreamState {
                stream_name: name,
                last_sequence: None, // No messages yet
                first_sequence: LogIndex::new(1).unwrap(),
                stats: StreamStats::default(),
            },
        );

        // Update metadata
        self.stream_count
            .store(self.group_stream_states.len(), Ordering::Relaxed);

        true
    }

    /// Remove a stream
    pub async fn remove_stream(&self, name: &StreamName) -> bool {
        if let Some((_, state)) = self.group_stream_states.remove(name) {
            // Update metadata
            self.stream_count
                .store(self.group_stream_states.len(), Ordering::Relaxed);
            self.total_messages
                .fetch_sub(state.stats.message_count, Ordering::Relaxed);
            self.total_bytes
                .fetch_sub(state.stats.total_bytes, Ordering::Relaxed);

            true
        } else {
            false
        }
    }

    /// Append messages to stream and return pre-serialized entries
    pub async fn append_to_group_stream(
        &self,
        stream: &StreamName,
        messages: Vec<Message>,
        timestamp_millis: u64,
    ) -> Arc<Vec<bytes::Bytes>> {
        if messages.is_empty() {
            return Arc::new(vec![]);
        }

        if let Some(mut state) = self.group_stream_states.get_mut(stream) {
            let mut entries = Vec::with_capacity(messages.len());
            // Calculate start sequence - if last_sequence is None (no messages), start at 1
            let start_sequence = match state.last_sequence {
                None => LogIndex::new(1).unwrap(),
                Some(last) => last.saturating_add(1),
            };

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
                        tracing::error!("Failed to serialize message: {}", e);
                        // Skip this message
                        continue;
                    }
                }
            }

            // Update state - last_sequence is the sequence of the last message appended
            if message_count > 0 {
                state.last_sequence = Some(start_sequence.saturating_add(message_count - 1));
            }
            state.stats.message_count += message_count;
            state.stats.total_bytes += total_size;
            state.stats.last_update = timestamp_millis / 1000; // Convert to seconds

            // Update metadata
            drop(state);
            self.total_messages
                .fetch_add(message_count, Ordering::Relaxed);
            self.total_bytes.fetch_add(total_size, Ordering::Relaxed);

            Arc::new(entries)
        } else {
            // Stream doesn't exist - this shouldn't happen as we validate in operations
            tracing::error!("Stream {} not found in state", stream);
            Arc::new(vec![])
        }
    }

    /// Trim stream up to sequence
    pub async fn trim_stream(&self, stream: &StreamName, up_to_seq: LogIndex) -> Option<LogIndex> {
        if let Some(mut state) = self.group_stream_states.get_mut(stream) {
            // Can only trim if up_to_seq is valid
            if up_to_seq >= state.first_sequence
                && state.last_sequence.is_some_and(|last| up_to_seq <= last)
            {
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
        sequence: LogIndex,
    ) -> Option<LogIndex> {
        if let Some(state) = self.group_stream_states.get(stream) {
            // Validate sequence is valid
            if state.last_sequence.is_none_or(|last| sequence > last) {
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
        self.group_stream_states
            .get(name)
            .map(|entry| entry.clone())
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamName> {
        self.group_stream_states
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get group metadata
    pub async fn get_metadata(&self) -> GroupMetadata {
        GroupMetadata {
            created_at: self.created_at.load(Ordering::Relaxed),
            stream_count: self.stream_count.load(Ordering::Relaxed),
            total_messages: self.total_messages.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
        }
    }

    /// Get stream count
    pub async fn stream_count(&self) -> usize {
        self.group_stream_states.len()
    }

    /// Get total message count
    pub async fn total_messages(&self) -> u64 {
        self.total_messages.load(Ordering::Relaxed)
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::new()
    }
}
