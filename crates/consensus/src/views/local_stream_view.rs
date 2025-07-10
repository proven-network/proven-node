//! Local stream view for read-only access to local consensus state
//!
//! This module provides read-only views of local stream state for monitoring
//! and decision-making without requiring consensus operations.

use crate::{
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Local stream health status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocalStreamHealth {
    /// Stream is healthy and accepting writes
    Healthy,
    /// Stream is paused (e.g., for migration)
    Paused,
    /// Stream is in read-only mode
    ReadOnly,
    /// Stream is experiencing issues
    Degraded {
        /// Reason for degraded state
        reason: String,
    },
    /// Stream health is unknown
    Unknown,
}

/// Local stream statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalStreamStats {
    /// Current sequence number
    pub current_sequence: u64,
    /// Total messages in the stream
    pub message_count: u64,
    /// Stream size in bytes
    pub size_bytes: u64,
    /// Number of pending operations
    pub pending_operations: usize,
    /// Last write timestamp
    pub last_write_timestamp: Option<u64>,
    /// Whether the stream is paused
    pub is_paused: bool,
}

/// Stream migration status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMigrationStatus {
    /// Source group ID
    pub source_group: ConsensusGroupId,
    /// Target group ID
    pub target_group: ConsensusGroupId,
    /// Migration phase
    pub phase: String,
    /// Progress percentage (0-100)
    pub progress: u8,
    /// Started timestamp
    pub started_at: u64,
    /// Last checkpoint sequence
    pub last_checkpoint_seq: u64,
}

/// Read-only view of local stream state
///
/// In a real implementation, this would have access to the local state machines
/// through a more abstract interface. For now, it's a placeholder.
pub struct LocalStreamView {
    /// Groups being managed locally
    local_groups: Vec<ConsensusGroupId>,
}

impl LocalStreamView {
    /// Create a new local stream view
    pub fn new(local_groups: Vec<ConsensusGroupId>) -> Self {
        Self { local_groups }
    }

    /// Get the health status of a stream in a specific group
    pub async fn get_stream_health(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<LocalStreamHealth> {
        if !self.local_groups.contains(&group_id) {
            return Err(Error::InvalidOperation(format!(
                "Group {:?} not found",
                group_id
            )));
        }

        // Check if stream exists in this group
        let streams = self.list_streams_in_group(group_id).await?;
        if !streams.contains(&stream_name.to_string()) {
            return Ok(LocalStreamHealth::Unknown);
        }

        // For now, return healthy unless we have more information
        // In a real implementation, would check stream metadata
        Ok(LocalStreamHealth::Healthy)
    }

    /// Get statistics for a stream in a specific group
    pub async fn get_stream_stats(
        &self,
        _group_id: ConsensusGroupId,
        _stream_name: &str,
    ) -> ConsensusResult<LocalStreamStats> {
        // In a real implementation, would query the state machine
        // For now, return default stats
        Ok(LocalStreamStats {
            current_sequence: 0,
            message_count: 0,
            size_bytes: 0,
            pending_operations: 0,
            last_write_timestamp: None,
            is_paused: false,
        })
    }

    /// List all streams in a consensus group
    pub async fn list_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>> {
        if !self.local_groups.contains(&group_id) {
            return Err(Error::InvalidOperation(format!(
                "Group {:?} not found",
                group_id
            )));
        }

        // In a real implementation, would query the state machine
        // For now, return empty list
        Ok(Vec::new())
    }

    /// Get all streams across all local groups
    pub async fn get_all_local_streams(
        &self,
    ) -> ConsensusResult<HashMap<String, ConsensusGroupId>> {
        let mut all_streams = HashMap::new();

        for group_id in &self.local_groups {
            let streams = self.list_streams_in_group(*group_id).await?;
            for stream in streams {
                all_streams.insert(stream, *group_id);
            }
        }

        Ok(all_streams)
    }

    /// Get migration status for a stream
    pub async fn get_migration_status(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<StreamMigrationStatus>> {
        // In a real implementation, would check migration state
        // For now, return None (no migration)
        let _ = stream_name;
        Ok(None)
    }

    /// Get aggregated statistics for a consensus group
    pub async fn get_group_stats(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GroupLocalStats> {
        let streams = self.list_streams_in_group(group_id).await?;
        let mut total_messages = 0;
        let mut total_size = 0;
        let mut total_pending = 0;

        for stream in &streams {
            let stats = self.get_stream_stats(group_id, stream).await?;
            total_messages += stats.message_count;
            total_size += stats.size_bytes;
            total_pending += stats.pending_operations;
        }

        Ok(GroupLocalStats {
            stream_count: streams.len(),
            total_messages,
            total_size_bytes: total_size,
            total_pending_operations: total_pending,
        })
    }

    /// Check if a group is ready for new streams
    pub async fn is_group_accepting_streams(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<bool> {
        // Group exists and is running
        Ok(self.local_groups.contains(&group_id))
    }

    /// Get the load factor for a group (0.0 - 1.0)
    pub async fn get_group_load_factor(&self, group_id: ConsensusGroupId) -> ConsensusResult<f64> {
        let stats = self.get_group_stats(group_id).await?;

        // Simple load calculation based on stream count
        // In real implementation, would consider message rate, size, etc.
        const MAX_STREAMS_PER_GROUP: usize = 1000;
        let load = stats.stream_count as f64 / MAX_STREAMS_PER_GROUP as f64;

        Ok(load.min(1.0))
    }
}

/// Aggregated statistics for a local consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupLocalStats {
    /// Number of streams in the group
    pub stream_count: usize,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Total pending operations
    pub total_pending_operations: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_stream_view_creation() {
        let view = LocalStreamView::new(vec![]);

        // Should return empty streams
        let streams = view.get_all_local_streams().await.unwrap();
        assert!(streams.is_empty());
    }

    #[tokio::test]
    async fn test_get_group_stats_empty() {
        let view = LocalStreamView::new(vec![]);

        // Should fail for non-existent group
        let result = view.get_group_stats(ConsensusGroupId::new(1)).await;
        assert!(result.is_err());
    }
}
