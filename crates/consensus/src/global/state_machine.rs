//! State machine implementation for global consensus
//!
//! This module provides the OpenRaft state machine implementation
//! using the new GlobalState and operations structure.

use super::{GlobalResponse, GlobalTypeConfig, global_state::GlobalState};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

// Re-export ConsensusGroupInfo from global_state
pub use super::global_state::ConsensusGroupInfo;

/// State machine for global consensus using operations
pub struct GlobalStateMachine {
    /// The global state
    state: Arc<GlobalState>,
    /// Last applied log ID
    last_applied_log: Arc<RwLock<Option<LogId<GlobalTypeConfig>>>>,
    /// Last membership config
    last_membership: Arc<RwLock<StoredMembership<GlobalTypeConfig>>>,
}

impl GlobalStateMachine {
    /// Create a new state machine
    pub fn new(state: Arc<GlobalState>) -> Self {
        Self {
            state,
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Get reference to the state
    pub fn state(&self) -> Arc<GlobalState> {
        self.state.clone()
    }
}

/// Snapshot builder for state machine
pub struct SnapshotBuilder {
    _state: Arc<GlobalState>,
}

impl RaftSnapshotBuilder<GlobalTypeConfig> for SnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<GlobalTypeConfig>, StorageError<GlobalTypeConfig>> {
        // For now, return an empty snapshot
        // In a real implementation, this would serialize the full state
        Ok(openraft::storage::Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: format!("snapshot-{}", chrono::Utc::now().timestamp()),
            },
            snapshot: Cursor::new(Vec::new()),
        })
    }
}

impl openraft::storage::RaftStateMachine<GlobalTypeConfig> for GlobalStateMachine {
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlobalTypeConfig>>,
            StoredMembership<GlobalTypeConfig>,
        ),
        StorageError<GlobalTypeConfig>,
    > {
        let last_applied = self.last_applied_log.read().await.clone();
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GlobalResponse>, StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(entry.log_id.clone());

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(GlobalResponse {
                        success: true,
                        sequence: entry.log_id.index,
                        error: None,
                    });
                }
                EntryPayload::Normal(request) => {
                    // Apply the operation directly
                    let response = self
                        .state
                        .apply_operation(&request.operation, entry.log_id.index)
                        .await;

                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    let mut last_membership = self.last_membership.write().await;
                    *last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), membership.clone());

                    responses.push(GlobalResponse {
                        success: true,
                        sequence: entry.log_id.index,
                        error: None,
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            _state: self.state.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlobalTypeConfig>,
        _snapshot: <GlobalTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        debug!("Installing snapshot with id: {}", meta.snapshot_id);

        // Update last applied log
        if let Some(log_id) = &meta.last_log_id {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(log_id.clone());
        }

        // Update membership
        let mut last_membership = self.last_membership.write().await;
        *last_membership = meta.last_membership.clone();

        // In a real implementation, we'd deserialize and restore the full state
        // For now, we just log the installation
        debug!("Snapshot installed successfully");

        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        <GlobalTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
        StorageError<GlobalTypeConfig>,
    > {
        Ok(Cursor::new(Vec::new()))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>>
    {
        // For now, return None - in production, we'd check if we have a stored snapshot
        Ok(None)
    }
}

/// Snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    /// Last applied log ID
    last_applied_log: Option<LogId<GlobalTypeConfig>>,
    /// Number of consensus groups
    group_count: usize,
    // In a real implementation, we'd include:
    // - All stream configurations
    // - All consensus group information
    // - Subject routing information
    // - etc.
}
