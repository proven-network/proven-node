//! Raft state machine implementation for global consensus
//!
//! This module handles applying committed entries to the business state.
//! It only processes entries that have been committed by Raft consensus.

use std::sync::Arc;

use openraft::{
    Entry, EntryPayload, LogId, StorageError, StoredMembership,
    storage::{RaftStateMachine, Snapshot},
};
use tokio::sync::RwLock;

use super::operations::{GlobalOperation, GlobalOperationHandler};
use super::raft::GlobalTypeConfig;
use super::snapshot::{GlobalSnapshot, GlobalSnapshotBuilder};
use super::state::GlobalState;
use super::types::{GlobalRequest, GlobalResponse};
use crate::foundation::traits::OperationHandler;
use proven_topology::NodeId;

/// Callback for state machine events
pub trait StateChangeCallback: Send + Sync {
    /// Called when a response is processed
    fn on_response_processed(&self, response: &GlobalResponse);

    /// Called when membership changes
    fn on_membership_changed(&self, new_members: &[NodeId], removed_members: &[NodeId]);
}

/// Raft state machine - handles applying committed entries
#[derive(Clone)]
pub struct GlobalStateMachine {
    /// Global state that gets modified by applied entries
    state: Arc<GlobalState>,
    /// Handler for processing operations
    handler: Arc<GlobalOperationHandler>,
    /// Last applied log index
    applied: Arc<RwLock<Option<LogId<GlobalTypeConfig>>>>,
    /// Current membership
    membership: Arc<RwLock<StoredMembership<GlobalTypeConfig>>>,
}

impl GlobalStateMachine {
    /// Create new state machine
    pub fn new(state: Arc<GlobalState>) -> Self {
        let handler = Arc::new(GlobalOperationHandler::new(state.clone()));

        Self {
            state,
            handler,
            applied: Arc::new(RwLock::new(None)),
            membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Get the state reference
    pub fn state(&self) -> &Arc<GlobalState> {
        &self.state
    }

    /// Apply entries with optional callback
    pub async fn apply_entries<I>(
        &self,
        entries: I,
        callback: Option<&dyn StateChangeCallback>,
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

            match &entry.payload {
                EntryPayload::Normal(req) => {
                    // Process the business logic
                    let operation = GlobalOperation::new(req.clone());
                    match self.handler.handle(operation).await {
                        Ok(response) => {
                            // Notify callback if provided
                            if let Some(cb) = callback {
                                cb.on_response_processed(&response);
                            }
                            responses.push(response);
                        }
                        Err(e) => responses.push(GlobalResponse::Error {
                            message: e.to_string(),
                        }),
                    }
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

                    if (!added_members.is_empty() || !removed_members.is_empty())
                        && let Some(cb) = callback
                    {
                        cb.on_membership_changed(&added_members, &removed_members);
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
        self.apply_entries(entries, None).await
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
    ) -> Result<Option<Snapshot<GlobalTypeConfig>>, StorageError<GlobalTypeConfig>> {
        // TODO: Create snapshot of current state
        // For now, return None indicating no snapshot available
        Ok(None)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        GlobalSnapshotBuilder::new(
            self.state.clone(),
            self.applied.read().await.clone(),
            self.membership.read().await.clone(),
        )
    }
}
