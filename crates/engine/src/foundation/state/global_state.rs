//! Global consensus state
//!
//! Pure state container for global consensus operations.
//! This module only manages state - all validation happens elsewhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use proven_storage::LogIndex;

use crate::Message;
use crate::foundation::StreamConfig;
use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::models::{GroupInfo, NodeInfo, StreamInfo, StreamState};
use crate::foundation::types::{ConsensusGroupId, StreamName};
use proven_topology::NodeId;

/// Global consensus state
#[derive(Clone)]
pub struct GlobalState {
    /// Stream configurations in groups
    stream_infos: Arc<DashMap<StreamName, StreamInfo>>,

    /// Consensus groups
    groups: Arc<DashMap<ConsensusGroupId, GroupInfo>>,

    /// Node to groups mapping
    node_groups: Arc<DashMap<NodeId, Vec<ConsensusGroupId>>>,

    /// Cluster members
    members: Arc<DashMap<NodeId, NodeInfo>>,

    /// Global stream states
    global_stream_states: Arc<DashMap<StreamName, StreamState>>,

    /// Metadata for global streams - using atomics for lock-free access
    stream_count: Arc<AtomicUsize>,
    total_messages: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
}

impl GlobalState {
    /// Create new global state
    pub fn new() -> Self {
        Self {
            stream_infos: Arc::new(DashMap::new()),
            groups: Arc::new(DashMap::new()),
            node_groups: Arc::new(DashMap::new()),
            members: Arc::new(DashMap::new()),
            global_stream_states: Arc::new(DashMap::new()),
            stream_count: Arc::new(AtomicUsize::new(0)),
            total_messages: Arc::new(AtomicU64::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Clear all state (used when installing snapshots)
    pub async fn clear(&self) {
        self.stream_infos.clear();
        self.groups.clear();
        self.node_groups.clear();
        self.members.clear();
    }

    // Stream operations

    /// Add a stream
    pub async fn add_stream(&self, info: StreamInfo) -> crate::error::ConsensusResult<()> {
        // If this is a global stream, initialize its state
        if matches!(info.placement, StreamPlacement::Global) {
            let stream_state = StreamState {
                stream_name: info.stream_name.clone(),
                last_sequence: None,
                first_sequence: LogIndex::new(1).unwrap(),
                stats: Default::default(),
            };
            self.global_stream_states
                .insert(info.stream_name.clone(), stream_state);
            self.stream_count.fetch_add(1, Ordering::Relaxed);
        }

        self.stream_infos.insert(info.stream_name.clone(), info);
        Ok(())
    }

    /// Remove a stream
    pub async fn remove_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        if let Some((_, info)) = self.stream_infos.remove(name) {
            // If this was a global stream, also remove its state
            if matches!(info.placement, StreamPlacement::Global) {
                self.global_stream_states.remove(name);
                self.stream_count.fetch_sub(1, Ordering::Relaxed);
            }
            Some(info)
        } else {
            None
        }
    }

    /// Get stream info
    pub async fn get_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        self.stream_infos.get(name).map(|entry| entry.clone())
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamInfo> {
        self.stream_infos
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all streams (alias for list_streams for consistency)
    pub async fn get_all_streams(&self) -> Vec<StreamInfo> {
        self.list_streams().await
    }

    /// Update stream config
    pub async fn update_stream_config(&self, name: &StreamName, config: StreamConfig) -> bool {
        if let Some(mut entry) = self.stream_infos.get_mut(name) {
            entry.config = config;
            true
        } else {
            false
        }
    }

    /// Reassign stream to different group
    pub async fn reassign_stream(&self, name: &StreamName, new_placement: StreamPlacement) -> bool {
        if let Some(mut entry) = self.stream_infos.get_mut(name) {
            entry.placement = new_placement;
            true
        } else {
            false
        }
    }

    /// Get global stream state
    pub async fn get_global_stream_state(&self, name: &StreamName) -> Option<StreamState> {
        self.global_stream_states
            .get(name)
            .map(|entry| entry.clone())
    }

    /// Append messages to stream and return pre-serialized entries and last sequence
    pub async fn append_to_global_stream(
        &self,
        stream: &StreamName,
        messages: Vec<Message>,
        timestamp_millis: u64,
    ) -> (Arc<Vec<bytes::Bytes>>, Option<LogIndex>) {
        if messages.is_empty() {
            return (Arc::new(vec![]), None);
        }

        if let Some(mut state) = self.global_stream_states.get_mut(stream) {
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
            let last_sequence = if message_count > 0 {
                let last_seq = start_sequence.saturating_add(message_count - 1);
                state.last_sequence = Some(last_seq);
                Some(last_seq)
            } else {
                state.last_sequence
            };

            state.stats.message_count += message_count;
            state.stats.total_bytes += total_size;
            state.stats.last_update = timestamp_millis / 1000; // Convert to seconds

            // Update metadata
            drop(state);
            self.total_messages
                .fetch_add(message_count, Ordering::Relaxed);
            self.total_bytes.fetch_add(total_size, Ordering::Relaxed);

            (Arc::new(entries), last_sequence)
        } else {
            // Stream doesn't exist - this shouldn't happen as we validate in operations
            tracing::error!("Stream {} not found in state", stream);
            (Arc::new(vec![]), None)
        }
    }

    /// Trim a global stream
    pub async fn trim_global_stream(
        &self,
        name: &StreamName,
        up_to_seq: LogIndex,
    ) -> Option<LogIndex> {
        if let Some(mut state) = self.global_stream_states.get_mut(name) {
            // Update first sequence to be the next sequence after trim point
            let new_first_seq = up_to_seq.saturating_add(1);

            // Only trim if the new first sequence is valid
            if let Some(last_seq) = state.last_sequence {
                if new_first_seq <= last_seq {
                    // Update message count (approximate - we don't track individual deletions)
                    let old_first_seq = state.first_sequence.get();
                    let trimmed_count = up_to_seq.get() - old_first_seq + 1;
                    state.stats.message_count =
                        state.stats.message_count.saturating_sub(trimmed_count);

                    state.first_sequence = new_first_seq;

                    Some(new_first_seq)
                } else {
                    // Can't trim beyond the last sequence
                    None
                }
            } else {
                // Stream is empty
                None
            }
        } else {
            None
        }
    }

    /// Delete a message from a global stream
    pub async fn delete_from_global_stream(
        &self,
        name: &StreamName,
        sequence: LogIndex,
    ) -> Option<LogIndex> {
        if let Some(mut state) = self.global_stream_states.get_mut(name) {
            // Check if sequence is in valid range
            if let Some(last_seq) = state.last_sequence {
                if sequence >= state.first_sequence && sequence <= last_seq {
                    // For now, we just decrement the message count
                    // In a real implementation, you'd mark the message as deleted in storage
                    state.stats.message_count = state.stats.message_count.saturating_sub(1);
                    Some(sequence)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    // Group operations

    /// Add a consensus group
    pub async fn add_group(&self, info: GroupInfo) -> crate::error::ConsensusResult<()> {
        self.groups.insert(info.id, info.clone());

        // Update node mappings
        for member in &info.members {
            self.node_groups
                .entry(member.clone())
                .or_default()
                .push(info.id);
        }
        Ok(())
    }

    /// Remove a consensus group
    pub async fn remove_group(&self, id: ConsensusGroupId) -> Option<GroupInfo> {
        if let Some((_, info)) = self.groups.remove(&id) {
            // Update node mappings
            for member in &info.members {
                if let Some(mut groups) = self.node_groups.get_mut(member) {
                    groups.retain(|&g| g != id);
                }
            }
            Some(info)
        } else {
            None
        }
    }

    /// Get group info
    pub async fn get_group(&self, id: &ConsensusGroupId) -> Option<GroupInfo> {
        self.groups.get(id).map(|entry| entry.clone())
    }

    /// List all groups
    pub async fn list_groups(&self) -> Vec<GroupInfo> {
        self.groups
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all groups (alias for list_groups)
    pub async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.list_groups().await
    }

    /// Add member to group
    pub async fn add_group_member(&self, group_id: ConsensusGroupId, node_id: NodeId) -> bool {
        if let Some(mut info) = self.groups.get_mut(&group_id) {
            if !info.members.contains(&node_id) {
                info.members.push(node_id.clone());

                // Update node mappings
                self.node_groups.entry(node_id).or_default().push(group_id);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Remove member from group
    pub async fn remove_group_member(&self, group_id: ConsensusGroupId, node_id: &NodeId) -> bool {
        if let Some(mut info) = self.groups.get_mut(&group_id) {
            let before = info.members.len();
            info.members.retain(|n| n != node_id);
            let removed = before != info.members.len();

            if removed {
                // Update node mappings
                if let Some(mut groups) = self.node_groups.get_mut(node_id) {
                    groups.retain(|&g| g != group_id);
                }
            }
            removed
        } else {
            false
        }
    }

    // Node operations

    /// Add cluster member
    pub async fn add_member(&self, info: NodeInfo) {
        self.members.insert(info.node_id.clone(), info);
    }

    /// Remove cluster member
    pub async fn remove_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.members.remove(node_id).map(|(_, v)| v)
    }

    /// Get member info
    pub async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.members.get(node_id).map(|entry| entry.clone())
    }

    /// List all members
    pub async fn list_members(&self) -> Vec<NodeInfo> {
        self.members
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get groups for a node
    pub async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        self.node_groups
            .get(node_id)
            .map(|entry| entry.clone())
            .unwrap_or_default()
    }

    // Query operations

    /// Count streams in a group
    pub async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize {
        self.stream_infos
            .iter()
            .filter(|entry| entry.value().placement == StreamPlacement::Group(group_id))
            .count()
    }

    /// Get streams for a group
    pub async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo> {
        self.stream_infos
            .iter()
            .filter(|entry| entry.value().placement == StreamPlacement::Group(group_id))
            .map(|entry| entry.value().clone())
            .collect()
    }
}

impl Default for GlobalState {
    fn default() -> Self {
        Self::new()
    }
}
