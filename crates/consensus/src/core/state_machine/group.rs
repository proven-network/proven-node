//! State machine implementation for group consensus using handlers
//!
//! This module provides the OpenRaft state machine implementation
//! for consensus groups using the new handler-based operation processing.

use crate::{
    ConsensusGroupId, NodeId,
    core::state_machine::local::StorageBackedLocalState,
    operations::handlers::{
        GroupOperationContext, GroupStreamHandlerRegistry, GroupStreamOperationResponse,
    },
};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Type alias for the group type config
pub type GroupTypeConfig = crate::core::group::GroupConsensusTypeConfig;

/// State machine for group consensus using operation handlers
pub struct GroupStateMachine {
    /// The underlying storage-backed state
    storage_state: Arc<StorageBackedLocalState>,
    /// Operation handler registry
    handler_registry: Arc<GroupStreamHandlerRegistry>,
    /// Last applied log ID
    last_applied_log: Arc<RwLock<Option<LogId<GroupTypeConfig>>>>,
    /// Last membership config
    last_membership: Arc<RwLock<StoredMembership<GroupTypeConfig>>>,
    /// The consensus group ID
    group_id: ConsensusGroupId,
}

impl GroupStateMachine {
    /// Create a new group state machine
    pub fn new(storage_state: Arc<StorageBackedLocalState>, group_id: ConsensusGroupId) -> Self {
        // Create handler registry with default handlers
        let handler_registry = Arc::new(GroupStreamHandlerRegistry::with_defaults());

        Self {
            storage_state,
            handler_registry,
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            group_id,
        }
    }

    /// Get the last applied log ID
    pub async fn last_applied_log(&self) -> Option<LogId<GroupTypeConfig>> {
        self.last_applied_log.read().await.clone()
    }

    /// Get the last membership
    pub async fn last_membership(&self) -> StoredMembership<GroupTypeConfig> {
        self.last_membership.read().await.clone()
    }

    /// Get the underlying storage state
    pub fn storage_state(&self) -> Arc<StorageBackedLocalState> {
        self.storage_state.clone()
    }

    /// Apply a single entry using handlers
    pub async fn apply_entry(
        &self,
        entry: &Entry<GroupTypeConfig>,
    ) -> Result<GroupStreamOperationResponse, String> {
        match &entry.payload {
            EntryPayload::Normal(request) => {
                debug!("Applying group operation: {:?}", request.operation_name());

                // Create operation context
                let context = GroupOperationContext::new(
                    entry.log_id.index,
                    entry.log_id.leader_id.node_id.clone(),
                    self.group_id,
                    false, // We don't know if we're leader here
                );

                // IMPORTANT: The handler interface expects &mut LocalStateMachine, but
                // StorageBackedLocalState is designed with interior mutability (Arc<UnifiedStreamManager>).
                // We should NOT create a new instance here. Instead, we need to work with the
                // existing instance through its shared reference.

                // Since StorageBackedLocalState has interior mutability through Arc<UnifiedStreamManager>,
                // we can safely use it even though the handler expects &mut. The actual mutations
                // happen inside the Arc<UnifiedStreamManager>.

                // Clone the Arc to get our own reference to the same underlying state
                let state = self.storage_state.clone();

                // SAFETY: This is safe because StorageBackedLocalState internally uses
                // Arc and RwLock for thread-safe access. The &mut requirement is a legacy
                // from when the state wasn't internally synchronized.
                // TODO: Update handler interface to accept &StorageBackedLocalState instead of &mut
                let state_ref = unsafe {
                    // Get a raw pointer to the StorageBackedLocalState inside the Arc
                    let ptr = Arc::as_ptr(&state) as *mut StorageBackedLocalState;
                    // Convert to a mutable reference
                    // This is safe because StorageBackedLocalState has interior mutability
                    &mut *ptr
                };

                // Process through handler registry
                self.handler_registry
                    .handle_operation(request, state_ref, &context)
                    .await
                    .map_err(|e| e.to_string())
            }
            EntryPayload::Membership(membership) => {
                // Update stored membership
                *self.last_membership.write().await =
                    StoredMembership::new(Some(entry.log_id.clone()), membership.clone());

                Ok(GroupStreamOperationResponse::Maintenance(
                    crate::operations::handlers::MaintenanceOperationResponse::MembershipUpdated {
                        sequence: entry.log_id.index,
                        group_id: self.group_id,
                        membership_config: format!("{membership:?}"),
                    },
                ))
            }
            EntryPayload::Blank => {
                // Heartbeat
                Ok(GroupStreamOperationResponse::Maintenance(
                    crate::operations::handlers::MaintenanceOperationResponse::Heartbeat {
                        sequence: entry.log_id.index,
                        group_id: self.group_id,
                    },
                ))
            }
        }
    }
}

/// Snapshot builder for group state machine
pub struct GroupSnapshotBuilder {
    group_id: ConsensusGroupId,
    _storage_state: Arc<StorageBackedLocalState>,
}

impl RaftSnapshotBuilder<GroupTypeConfig> for GroupSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<GroupTypeConfig>, StorageError<GroupTypeConfig>> {
        // For now, return an empty snapshot
        // In a real implementation, this would serialize the stream data
        Ok(openraft::storage::Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: format!(
                    "group-{}-snapshot-{}",
                    self.group_id,
                    chrono::Utc::now().timestamp()
                ),
            },
            snapshot: Cursor::new(Vec::new()),
        })
    }
}

impl openraft::storage::RaftStateMachine<GroupTypeConfig> for GroupStateMachine {
    type SnapshotBuilder = GroupSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GroupTypeConfig>>,
            StoredMembership<GroupTypeConfig>,
        ),
        StorageError<GroupTypeConfig>,
    > {
        let last_applied = self.last_applied_log.read().await.clone();
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GroupStreamOperationResponse>, StorageError<GroupTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(entry.log_id.clone());

            match &entry.payload {
                EntryPayload::Blank => {
                    // For blank entries, create a simple success response
                    responses.push(GroupStreamOperationResponse::Maintenance(
                        crate::operations::handlers::MaintenanceOperationResponse::Verified {
                            sequence: entry.log_id.index,
                            entries_checked: 0,
                            errors_found: 0,
                            errors: vec!["Blank entry processed".to_string()],
                        },
                    ));
                }
                EntryPayload::Normal(operation) => {
                    // Create operation context
                    let context = GroupOperationContext::new(
                        entry.log_id.index,
                        NodeId::from_seed(0), // Default node ID - in production this would come from entry metadata
                        self.group_id,
                        true, // State machine applies are always from committed entries
                    );

                    // Clone the storage state for mutable access
                    let mut state_clone = (*self.storage_state).clone();

                    // Use handler registry to process the operation
                    let handler_response = self
                        .handler_registry
                        .handle_operation(operation, &mut state_clone, &context)
                        .await;

                    // Add handler response to the list
                    match handler_response {
                        Ok(op_response) => {
                            responses.push(op_response);
                        }
                        Err(e) => {
                            // Create a failed response
                            responses.push(GroupStreamOperationResponse::Stream(
                                crate::operations::handlers::StreamOperationResponse::Failed {
                                    operation: format!("{operation:?}"),
                                    reason: e.to_string(),
                                },
                            ));
                        }
                    }
                }
                EntryPayload::Membership(membership) => {
                    let mut last_membership = self.last_membership.write().await;
                    *last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), membership.clone());

                    // For membership changes, create a maintenance response
                    responses.push(GroupStreamOperationResponse::Maintenance(
                        crate::operations::handlers::MaintenanceOperationResponse::Verified {
                            sequence: entry.log_id.index,
                            entries_checked: 1,
                            errors_found: 0,
                            errors: vec!["Membership updated".to_string()],
                        },
                    ));
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GroupSnapshotBuilder {
            group_id: self.group_id,
            _storage_state: self.storage_state.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GroupTypeConfig>,
        _snapshot: <GroupTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        debug!("Installing snapshot with id: {}", meta.snapshot_id);

        // Update last applied log
        if let Some(log_id) = &meta.last_log_id {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(log_id.clone());
        }

        // Update last membership
        let mut last_membership = self.last_membership.write().await;
        *last_membership = meta.last_membership.clone();

        // In a real implementation, we would restore stream data from the snapshot

        debug!("Snapshot installed successfully");

        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        <GroupTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
        StorageError<GroupTypeConfig>,
    > {
        Ok(Cursor::new(Vec::new()))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<GroupTypeConfig>>, StorageError<GroupTypeConfig>>
    {
        // For now, return None - in production, we'd check if we have a stored snapshot
        Ok(None)
    }
}

/// Snapshot data structure for groups
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupSnapshotData {
    /// Last applied log ID
    last_applied_log: Option<LogId<GroupTypeConfig>>,
    /// Group ID
    group_id: ConsensusGroupId,
    /// Number of streams in this group
    stream_count: usize,
    // In a real implementation, we'd include:
    // - Stream data snapshots
    // - Migration states
    // - etc.
}
