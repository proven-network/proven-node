//! Raft state machine implementation for global consensus
//!
//! This module handles applying committed entries to the business state.
//! It only processes entries that have been committed by Raft consensus.

use std::{num::NonZero, sync::Arc};

use openraft::{
    Entry, EntryPayload, LogId, StorageError, StoredMembership,
    storage::{RaftStateMachine, Snapshot},
};
use tokio::sync::RwLock;

use super::callbacks::GlobalConsensusCallbacks;
use super::dispatcher::GlobalCallbackDispatcher;
use super::operations::{GlobalOperation, GlobalOperationHandler};
use super::raft::GlobalTypeConfig;
use super::snapshot::{GlobalSnapshot, GlobalSnapshotBuilder};
use super::types::{GlobalRequest, GlobalResponse};
use crate::foundation::{
    GlobalStateWriter, global_state::GlobalState, state_access::GlobalStateWrite,
    traits::OperationHandler,
};
use proven_topology::NodeId;

/// Raft state machine - handles applying committed entries
#[derive(Clone)]
pub struct GlobalStateMachine {
    /// Global state that gets modified by applied entries
    state: GlobalStateWriter,
    /// Handler for processing operations
    handler: Arc<GlobalOperationHandler>,
    /// Callback dispatcher
    callback_dispatcher: Arc<GlobalCallbackDispatcher>,
    /// Last applied log index
    applied: Arc<RwLock<Option<LogId<GlobalTypeConfig>>>>,
    /// Current membership
    membership: Arc<RwLock<StoredMembership<GlobalTypeConfig>>>,
    /// Last log index that was persisted before this instance started
    replay_boundary: Option<NonZero<u64>>,
    /// Whether we've fired the state sync callback
    state_synced: Arc<RwLock<bool>>,
}

impl GlobalStateMachine {
    /// Create new state machine with handler and dispatcher
    pub fn new(
        state: GlobalStateWriter,
        handler: Arc<GlobalOperationHandler>,
        callback_dispatcher: Arc<GlobalCallbackDispatcher>,
        replay_boundary: Option<NonZero<u64>>,
    ) -> Self {
        Self {
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
    pub fn state(&self) -> &GlobalStateWriter {
        &self.state
    }

    /// Get the operation handler reference
    pub fn handler(&self) -> &Arc<GlobalOperationHandler> {
        &self.handler
    }

    /// Get the callback dispatcher reference
    pub fn callback_dispatcher(&self) -> &Arc<GlobalCallbackDispatcher> {
        &self.callback_dispatcher
    }

    /// Apply entries with optional callback
    pub async fn apply_entries<I>(
        &self,
        entries: I,
    ) -> Result<Vec<GlobalResponse>, StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            // Update applied index
            let log_id = entry.log_id.clone();
            *self.applied.write().await = Some(log_id.clone());

            tracing::info!(
                "Applying log entry: index={}, leader_id={:?}",
                log_id.index,
                log_id.leader_id
            );

            // Check if we've crossed the replay boundary
            if let Some(boundary) = self.replay_boundary
                && log_id.index + 1 >= boundary.get()
                && !*self.state_synced.read().await
            {
                // We've reached the replay boundary - fire state sync callback
                *self.state_synced.write().await = true;

                // Dispatch state sync callback
                // Important: We don't hold any locks when dispatching to avoid deadlocks
                self.callback_dispatcher.dispatch_state_sync().await;
            }

            match &entry.payload {
                EntryPayload::Normal(req) => {
                    // Process the business logic
                    let operation = GlobalOperation::new(req.clone());
                    // Check if this is a replay operation
                    // Convert 0-based log_id.index to 1-based for comparison with boundary
                    let is_replay = self
                        .replay_boundary
                        .map(|boundary| log_id.index + 1 < boundary.get())
                        .unwrap_or(false);

                    let response = match self.handler.handle(operation.clone(), is_replay).await {
                        Ok(resp) => resp,
                        Err(e) => GlobalResponse::Error {
                            message: e.to_string(),
                        },
                    };

                    // Dispatch callbacks based on operation result
                    self.callback_dispatcher
                        .dispatch_operation(&operation.request, &response, is_replay)
                        .await;

                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    // Check if this is a replay operation
                    // Convert 0-based log_id.index to 1-based for comparison with boundary
                    let is_replay = self
                        .replay_boundary
                        .map(|boundary| log_id.index + 1 < boundary.get())
                        .unwrap_or(false);

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
                            .dispatch_membership_changed(&added_members, &removed_members)
                            .await;
                    }

                    // OpenRaft expects a response for every entry
                    responses.push(GlobalResponse::Success);
                }
                EntryPayload::Blank => {
                    // Blank entries are used for leader election
                    // OpenRaft expects a response for every entry
                    responses.push(GlobalResponse::Success);
                }
            }
        }

        Ok(responses)
    }
}

impl RaftStateMachine<GlobalTypeConfig> for Arc<GlobalStateMachine> {
    type SnapshotBuilder = GlobalSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlobalTypeConfig>>,
            StoredMembership<GlobalTypeConfig>,
        ),
        StorageError<GlobalTypeConfig>,
    > {
        let applied = self.applied.read().await.clone();
        let membership = self.membership.read().await.clone();
        Ok((applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GlobalResponse>, StorageError<GlobalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Apply entries without callback (trait requirement)
        self.apply_entries(entries).await
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<GlobalSnapshot, StorageError<GlobalTypeConfig>> {
        // Create an empty snapshot to receive data
        Ok(GlobalSnapshot::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<GlobalTypeConfig>,
        mut snapshot: GlobalSnapshot,
    ) -> Result<(), StorageError<GlobalTypeConfig>> {
        tracing::info!(
            "Installing snapshot: id={}, last_log_id={:?}",
            meta.snapshot_id,
            meta.last_log_id
        );

        // Install snapshot into state machine
        use tokio::io::AsyncReadExt;
        let mut buffer = Vec::new();
        snapshot
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // 1. Deserialize the snapshot data
        let snapshot_data: super::snapshot::GlobalSnapshotData =
            ciborium::from_reader(&buffer[..]).map_err(|e| StorageError::read(&e))?;

        // Verify snapshot integrity
        if !snapshot_data.verify() {
            use crate::error::{Error, ErrorKind};
            return Err(StorageError::read(&Error::with_context(
                ErrorKind::InvalidState,
                "Snapshot checksum verification failed",
            )));
        }

        // Deserialize the state data
        let state_data: super::snapshot::StateSnapshotData =
            ciborium::from_reader(&snapshot_data.state[..]).map_err(|e| StorageError::read(&e))?;

        // 2. Replace the current state with snapshot state
        // Clear existing state first
        self.state.clear().await;

        // Store counts for logging
        let group_count = state_data.groups.len();
        let stream_count = state_data.streams.len();

        // Install groups
        for group in state_data.groups {
            self.state
                .add_group(group)
                .await
                .map_err(|e| StorageError::write(&e))?;
        }

        // Install streams
        for stream in state_data.streams {
            self.state
                .add_stream(stream)
                .await
                .map_err(|e| StorageError::write(&e))?;
        }

        // 3. Update applied index and membership from metadata
        *self.applied.write().await = meta.last_log_id.clone();
        *self.membership.write().await = meta.last_membership.clone();

        // 4. Mark state as synced and fire the state sync callback
        *self.state_synced.write().await = true;
        // Important: We don't hold any locks when dispatching to avoid deadlocks
        self.callback_dispatcher.dispatch_state_sync().await;

        tracing::info!(
            "Installed snapshot with {} groups and {} streams",
            group_count,
            stream_count
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        // Build a snapshot of the current state
        let mut builder = self.get_snapshot_builder().await;
        use openraft::storage::RaftSnapshotBuilder;
        let snapshot = builder.build_snapshot().await?;
        Ok(Some(snapshot))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GlobalSnapshotBuilder::new(
            self.state.clone(),
            self.applied.read().await.clone(),
            self.membership.read().await.clone(),
        )
    }
}
