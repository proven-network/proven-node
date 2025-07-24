//! Snapshot implementation for global consensus
//!
//! This module handles creating and restoring snapshots of the global state.

use std::sync::Arc;

use crate::foundation::{GlobalState, GlobalStateRead, GlobalStateWriter};
use openraft::{
    LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, storage::Snapshot,
};
use serde::{Deserialize, Serialize};

use super::raft::GlobalTypeConfig;

/// Global consensus snapshot - implements AsyncRead/Write/Seek for OpenRaft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSnapshot {
    /// Serialized global state
    pub data: Vec<u8>,
    /// Current read position
    #[serde(skip)]
    position: usize,
}

impl GlobalSnapshot {
    /// Create a new snapshot
    pub fn new(data: Vec<u8>) -> Self {
        Self { data, position: 0 }
    }
}

// Implement AsyncRead for GlobalSnapshot
impl tokio::io::AsyncRead for GlobalSnapshot {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let remaining = &self.data[self.position..];
        let to_read = std::cmp::min(remaining.len(), buf.remaining());
        buf.put_slice(&remaining[..to_read]);
        self.position += to_read;
        std::task::Poll::Ready(Ok(()))
    }
}

// Implement AsyncWrite for GlobalSnapshot
impl tokio::io::AsyncWrite for GlobalSnapshot {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.data.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

// Implement AsyncSeek for GlobalSnapshot
impl tokio::io::AsyncSeek for GlobalSnapshot {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let new_pos = match position {
            std::io::SeekFrom::Start(pos) => pos as usize,
            std::io::SeekFrom::End(pos) => (self.data.len() as i64 + pos) as usize,
            std::io::SeekFrom::Current(pos) => (self.position as i64 + pos) as usize,
        };
        self.position = new_pos.min(self.data.len());
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.position as u64))
    }
}

impl std::marker::Unpin for GlobalSnapshot {}

/// Global snapshot builder
pub struct GlobalSnapshotBuilder {
    /// State to snapshot
    state: GlobalStateWriter,
    /// Last applied log ID
    last_applied: Option<LogId<GlobalTypeConfig>>,
    /// Current membership
    membership: StoredMembership<GlobalTypeConfig>,
}

impl GlobalSnapshotBuilder {
    /// Create new snapshot builder
    pub fn new(
        state: GlobalStateWriter,
        last_applied: Option<LogId<GlobalTypeConfig>>,
        membership: StoredMembership<GlobalTypeConfig>,
    ) -> Self {
        Self {
            state,
            last_applied,
            membership,
        }
    }
}

impl RaftSnapshotBuilder<GlobalTypeConfig> for GlobalSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GlobalTypeConfig>, StorageError<GlobalTypeConfig>> {
        // 1. Get all groups and streams
        let all_groups = self.state.get_all_groups().await;
        let all_streams = self.state.get_all_streams().await;

        // 2. Create a state snapshot structure
        let state_data = StateSnapshotData {
            groups: all_groups,
            streams: all_streams,
        };

        // 3. Serialize to a compact format
        let mut serialized_state = Vec::new();
        ciborium::into_writer(&state_data, &mut serialized_state)
            .map_err(|e| StorageError::write(&e))?;

        // Create snapshot data with metadata
        let snapshot_data = GlobalSnapshotData::new(
            serialized_state,
            state_data.streams.len(),
            state_data.groups.len(),
        );

        // Serialize the complete snapshot
        let mut snapshot_bytes = Vec::new();
        ciborium::into_writer(&snapshot_data, &mut snapshot_bytes)
            .map_err(|e| StorageError::write(&e))?;

        let snapshot = GlobalSnapshot::new(snapshot_bytes);

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied.clone(),
                last_membership: self.membership.clone(),
                snapshot_id: format!("global-snapshot-{}", chrono::Utc::now().timestamp()),
            },
            snapshot,
        })
    }
}

/// State snapshot data containing all groups and streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshotData {
    /// All groups in the system
    pub groups: Vec<crate::foundation::GroupInfo>,
    /// All streams in the system
    pub streams: Vec<crate::foundation::models::StreamInfo>,
}

/// Snapshot data structure that would be serialized
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSnapshotData {
    /// Snapshot version for compatibility
    pub version: u32,
    /// Timestamp when snapshot was taken
    pub timestamp: i64,
    /// Serialized global state
    pub state: Vec<u8>,
    /// Additional metadata
    pub metadata: SnapshotMetadata,
}

/// Snapshot metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Number of streams in snapshot
    pub stream_count: usize,
    /// Number of groups in snapshot
    pub group_count: usize,
    /// Checksum for verification
    pub checksum: u64,
}

impl GlobalSnapshotData {
    /// Current snapshot version
    pub const CURRENT_VERSION: u32 = 1;

    /// Create new snapshot data
    pub fn new(state: Vec<u8>, stream_count: usize, group_count: usize) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        state.hash(&mut hasher);
        let checksum = hasher.finish();

        Self {
            version: Self::CURRENT_VERSION,
            timestamp: chrono::Utc::now().timestamp(),
            state,
            metadata: SnapshotMetadata {
                stream_count,
                group_count,
                checksum,
            },
        }
    }

    /// Verify snapshot integrity
    pub fn verify(&self) -> bool {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.state.hash(&mut hasher);
        hasher.finish() == self.metadata.checksum
    }
}
