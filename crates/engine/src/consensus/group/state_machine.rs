//! Raft state machine implementation for group consensus
//!
//! This module handles applying committed entries to the business state.
//! It only processes entries that have been committed by Raft consensus.

use std::sync::Arc;

use openraft::{
    Entry, EntryPayload, LogId, StorageError, StoredMembership,
    storage::{RaftStateMachine, Snapshot},
};
use tokio::sync::RwLock;

use super::callbacks::GroupConsensusCallbacks;
use super::dispatcher::GroupCallbackDispatcher;
use super::operations::{GroupOperation, GroupOperationHandler};
use super::raft::GroupTypeConfig;
use super::snapshot::{GroupSnapshot, GroupSnapshotBuilder};
use super::state::GroupState;
use super::types::{GroupRequest, GroupResponse, StreamOperation};
use crate::foundation::{traits::OperationHandler, types::ConsensusGroupId};
use proven_topology::NodeId;

/// Raft state machine - handles applying committed entries
#[derive(Clone)]
pub struct GroupStateMachine {
    /// Group ID
    group_id: ConsensusGroupId,
    /// Group state that gets modified by applied entries
    state: Arc<GroupState>,
    /// Handler for processing operations
    handler: Arc<GroupOperationHandler>,
    /// Callback dispatcher
    callback_dispatcher: Arc<GroupCallbackDispatcher>,
    /// Last applied log index
    applied: Arc<RwLock<Option<LogId<GroupTypeConfig>>>>,
    /// Current membership
    membership: Arc<RwLock<StoredMembership<GroupTypeConfig>>>,
    /// Last log index that was persisted before this instance started
    replay_boundary: Option<u64>,
    /// Whether we've fired the state sync callback
    state_synced: Arc<RwLock<bool>>,
}

impl GroupStateMachine {
    /// Create new state machine with handler and dispatcher
    pub fn new(
        group_id: ConsensusGroupId,
        state: Arc<GroupState>,
        handler: Arc<GroupOperationHandler>,
        callback_dispatcher: Arc<GroupCallbackDispatcher>,
        replay_boundary: Option<u64>,
    ) -> Self {
        Self {
            group_id,
            state,
            handler,
            callback_dispatcher,
            applied: Arc::new(RwLock::new(None)),
            membership: Arc::new(RwLock::new(StoredMembership::default())),
            replay_boundary,
            state_synced: Arc::new(RwLock::new(false)),
        }
    }

    /// Get the state reference
    pub fn state(&self) -> &Arc<GroupState> {
        &self.state
    }

    /// Get the operation handler reference
    pub fn handler(&self) -> &Arc<GroupOperationHandler> {
        &self.handler
    }

    /// Get the callback dispatcher reference
    pub fn callback_dispatcher(&self) -> &Arc<GroupCallbackDispatcher> {
        &self.callback_dispatcher
    }

    /// Apply entries
    pub async fn apply_entries<I>(
        &self,
        entries: I,
    ) -> Result<Vec<GroupResponse>, StorageError<GroupTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            // Update applied index
            let log_id = entry.log_id.clone();
            *self.applied.write().await = Some(log_id.clone());

            // Check if we've crossed the replay boundary
            if let Some(boundary) = self.replay_boundary
                && log_id.index > boundary
                && !*self.state_synced.read().await
            {
                // We've crossed into current operations - fire state sync callback
                *self.state_synced.write().await = true;

                // Dispatch state sync callback
                self.callback_dispatcher
                    .dispatch_state_sync(self.group_id, &self.state)
                    .await;
            }

            // Check if this is a replay operation
            let is_replay = self
                .replay_boundary
                .map(|boundary| log_id.index <= boundary)
                .unwrap_or(false);

            match &entry.payload {
                EntryPayload::Normal(req) => {
                    // Process the business logic
                    let operation = GroupOperation::new(req.clone());
                    let response = match self.handler.handle(operation.clone(), is_replay).await {
                        Ok(resp) => resp,
                        Err(e) => GroupResponse::Error {
                            message: e.to_string(),
                        },
                    };

                    // Dispatch callbacks based on operation result
                    self.callback_dispatcher
                        .dispatch_operation(self.group_id, &operation.request, &response, is_replay)
                        .await;

                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    // Update membership
                    let old_membership = self.membership.read().await.clone();
                    *self.membership.write().await =
                        StoredMembership::new(Some(log_id), membership.clone());

                    // Calculate membership changes
                    let old_members: Vec<NodeId> =
                        old_membership.nodes().map(|(id, _)| id.clone()).collect();

                    let new_members: Vec<NodeId> =
                        membership.nodes().map(|(id, _)| id.clone()).collect();

                    // Find removed members
                    let removed_members: Vec<NodeId> = old_members
                        .iter()
                        .filter(|id| !new_members.contains(id))
                        .cloned()
                        .collect();

                    // Find added members
                    let added_members: Vec<NodeId> = new_members
                        .iter()
                        .filter(|id| !old_members.contains(id))
                        .cloned()
                        .collect();

                    if (!added_members.is_empty() || !removed_members.is_empty()) && !is_replay {
                        // Dispatch membership change callbacks for current operations
                        self.callback_dispatcher
                            .dispatch_membership_changed(
                                self.group_id,
                                &added_members,
                                &removed_members,
                            )
                            .await;
                    }

                    // OpenRaft expects a response for every entry
                    responses.push(GroupResponse::Success);
                }
                EntryPayload::Blank => {
                    // Blank entries are used for leader election
                    // OpenRaft expects a response for every entry
                    responses.push(GroupResponse::Success);
                }
            }
        }

        Ok(responses)
    }
}

impl RaftStateMachine<GroupTypeConfig> for Arc<GroupStateMachine> {
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
        let applied = self.applied.read().await.clone();
        let membership = self.membership.read().await.clone();
        Ok((applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GroupResponse>, StorageError<GroupTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Apply entries
        self.apply_entries(entries).await
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<GroupSnapshot, StorageError<GroupTypeConfig>> {
        // Create an empty snapshot to receive data
        Ok(GroupSnapshot::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<GroupTypeConfig>,
        mut snapshot: GroupSnapshot,
    ) -> Result<(), StorageError<GroupTypeConfig>> {
        // Install snapshot into state machine
        use tokio::io::AsyncReadExt;
        let mut buffer = Vec::new();
        snapshot
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // TODO: Deserialize and install state
        // This should:
        // 1. Deserialize the snapshot data
        // 2. Replace the current state with snapshot state
        // 3. Update applied index and membership from metadata

        // Update metadata
        *self.applied.write().await = meta.last_log_id.clone();
        *self.membership.write().await = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GroupTypeConfig>>, StorageError<GroupTypeConfig>> {
        // TODO: Create snapshot of current state
        // For now, return None indicating no snapshot available
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GroupSnapshotBuilder::new(
            self.state.clone(),
            self.applied.read().await.clone(),
            self.membership.read().await.clone(),
        )
    }
}
