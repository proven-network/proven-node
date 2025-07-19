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

use super::operations::{GroupOperation, GroupOperationHandler};
use super::raft::GroupTypeConfig;
use super::snapshot::{GroupSnapshot, GroupSnapshotBuilder};
use super::state::GroupState;
use super::types::{GroupRequest, GroupResponse, StreamOperation};
use crate::foundation::traits::OperationHandler;
use proven_topology::NodeId;

/// Callback for state machine events
pub trait StateChangeCallback: Send + Sync {
    /// Called when a stream operation is processed
    fn on_stream_operation(
        &self,
        stream_name: &str,
        operation: &StreamOperation,
        response: &GroupResponse,
    );

    /// Called when membership changes
    fn on_membership_changed(&self, new_members: &[NodeId], removed_members: &[NodeId]);
}

/// Raft state machine - handles applying committed entries
#[derive(Clone)]
pub struct GroupStateMachine {
    /// Group state that gets modified by applied entries
    state: Arc<GroupState>,
    /// Handler for processing operations
    handler: Arc<GroupOperationHandler>,
    /// Last applied log index
    applied: Arc<RwLock<Option<LogId<GroupTypeConfig>>>>,
    /// Current membership
    membership: Arc<RwLock<StoredMembership<GroupTypeConfig>>>,
}

impl GroupStateMachine {
    /// Create new state machine
    pub fn new(state: Arc<GroupState>) -> Self {
        let handler = Arc::new(GroupOperationHandler::new(state.clone()));

        Self {
            state,
            handler,
            applied: Arc::new(RwLock::new(None)),
            membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Get the state reference
    pub fn state(&self) -> &Arc<GroupState> {
        &self.state
    }

    /// Apply entries with optional callback
    pub async fn apply_entries<I>(
        &self,
        entries: I,
        callback: Option<&dyn StateChangeCallback>,
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

            match &entry.payload {
                EntryPayload::Normal(req) => {
                    // Process the business logic
                    let operation = GroupOperation::new(req.clone());
                    match self.handler.handle(operation).await {
                        Ok(response) => {
                            // Notify callback if provided
                            if let (Some(cb), GroupRequest::Stream(stream_op)) = (callback, req) {
                                let stream_name = match stream_op {
                                    StreamOperation::Append { stream, .. } => stream.to_string(),
                                    StreamOperation::Trim { stream, .. } => stream.to_string(),
                                    StreamOperation::Delete { stream, .. } => stream.to_string(),
                                };
                                cb.on_stream_operation(&stream_name, stream_op, &response);
                            }
                            responses.push(response);
                        }
                        Err(e) => responses.push(GroupResponse::Error {
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
        // Apply entries without callback (trait requirement)
        self.apply_entries(entries, None).await
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
