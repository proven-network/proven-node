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
use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{Event, EventPublisher};
use proven_topology::NodeId;

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
    /// Event publisher for consensus events
    event_publisher: Arc<RwLock<Option<EventPublisher>>>,
    /// Node ID for event source
    node_id: Arc<RwLock<Option<NodeId>>>,
    /// Group ID for this state machine
    group_id: Arc<RwLock<Option<ConsensusGroupId>>>,
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
            event_publisher: Arc::new(RwLock::new(None)),
            node_id: Arc::new(RwLock::new(None)),
            group_id: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the event publisher and identifiers
    pub async fn set_event_context(
        &self,
        publisher: EventPublisher,
        node_id: NodeId,
        group_id: ConsensusGroupId,
    ) {
        *self.event_publisher.write().await = Some(publisher);
        *self.node_id.write().await = Some(node_id);
        *self.group_id.write().await = Some(group_id);
    }

    /// Get the state reference
    pub fn state(&self) -> &Arc<GroupState> {
        &self.state
    }

    /// Get the event publisher if set
    pub async fn event_publisher(&self) -> Option<EventPublisher> {
        self.event_publisher.read().await.clone()
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
        // Apply committed entries to state machine
        let mut responses = Vec::new();

        for entry in entries {
            // Update applied index
            let log_id = entry.log_id.clone();
            *self.applied.write().await = Some(log_id.clone());

            match &entry.payload {
                EntryPayload::Normal(req) => {
                    // Process the business logic
                    let operation = GroupOperation::new(req.clone());

                    match self.handler.handle(operation.clone()).await {
                        Ok(response) => {
                            // Publish event based on response
                            let publisher = self.event_publisher.read().await;
                            let node_id = self.node_id.read().await;
                            let group_id = self.group_id.read().await;

                            if let (Some(publisher), Some(node_id), Some(group_id)) =
                                (publisher.as_ref(), node_id.as_ref(), group_id.as_ref())
                            {
                                // Generate events based on the response type
                                let event = match &response {
                                    GroupResponse::Appended { stream, sequence } => {
                                        // Extract message data from the original request
                                        if let GroupRequest::Stream(StreamOperation::Append {
                                            message,
                                            ..
                                        }) = &operation.request
                                        {
                                            Some(Event::StreamMessageAppended {
                                                stream: stream.clone(),
                                                group_id: *group_id,
                                                sequence: *sequence,
                                                message: message.clone(),
                                                timestamp: operation.timestamp,
                                                term: log_id.leader_id.term,
                                            })
                                        } else {
                                            None
                                        }
                                    }
                                    GroupResponse::Trimmed {
                                        stream,
                                        new_start_seq,
                                    } => Some(Event::StreamTrimmed {
                                        stream: stream.clone(),
                                        group_id: *group_id,
                                        new_start_seq: *new_start_seq,
                                    }),
                                    _ => None,
                                };

                                if let Some(event) = event {
                                    // Fire and forget - we don't want to block consensus on event publishing
                                    let publisher = publisher.clone();
                                    let source = format!("group-consensus-{group_id}-{node_id}");
                                    tokio::spawn(async move {
                                        if let Err(e) = publisher.publish(event, source).await {
                                            tracing::warn!(
                                                "Failed to publish consensus event: {}",
                                                e
                                            );
                                        }
                                    });
                                }
                            }

                            responses.push(response)
                        }
                        Err(e) => responses.push(GroupResponse::Error {
                            message: e.to_string(),
                        }),
                    }
                }
                EntryPayload::Membership(membership) => {
                    // Update membership
                    *self.membership.write().await =
                        StoredMembership::new(Some(log_id), membership.clone());

                    // Publish membership change event
                    let publisher = self.event_publisher.read().await;
                    let node_id = self.node_id.read().await;
                    let group_id = self.group_id.read().await;

                    if let (Some(publisher), Some(node_id), Some(group_id)) =
                        (publisher.as_ref(), node_id.as_ref(), group_id.as_ref())
                    {
                        let event = Event::MembershipChanged {
                            new_members: membership
                                .nodes()
                                .map(|(node_id, _)| node_id.clone())
                                .collect(),
                            removed_members: vec![], // Would need to track previous membership
                        };

                        let publisher = publisher.clone();
                        let source = format!("group-consensus-{group_id}-{node_id}");
                        tokio::spawn(async move {
                            if let Err(e) = publisher.publish(event, source).await {
                                tracing::warn!("Failed to publish membership event: {}", e);
                            }
                        });
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
