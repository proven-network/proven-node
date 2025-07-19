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
use crate::services::event::{Event, EventPublisher, EventResult};
use proven_topology::NodeId;

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
    /// Event publisher for consensus events
    event_publisher: Arc<RwLock<Option<EventPublisher>>>,
    /// Node ID for event source
    node_id: Arc<RwLock<Option<NodeId>>>,
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
            event_publisher: Arc::new(RwLock::new(None)),
            node_id: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the event publisher
    pub async fn set_event_publisher(&self, publisher: EventPublisher, node_id: NodeId) {
        *self.event_publisher.write().await = Some(publisher);
        *self.node_id.write().await = Some(node_id);
    }

    /// Get the state reference
    pub fn state(&self) -> &Arc<GlobalState> {
        &self.state
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
        // Apply committed entries to state machine
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
                            // Publish event based on response
                            let publisher = self.event_publisher.read().await;
                            let node_id = self.node_id.read().await;
                            if let (Some(publisher), Some(node_id)) =
                                (publisher.as_ref(), node_id.as_ref())
                            {
                                let event = match &response {
                                    GlobalResponse::StreamCreated { name, group_id } => {
                                        // Get config from state for the event
                                        if let Some(stream_info) = self.state.get_stream(name).await
                                        {
                                            Some(Event::StreamCreated {
                                                name: name.clone(),
                                                config: stream_info.config.clone(),
                                                group_id: *group_id,
                                            })
                                        } else {
                                            None
                                        }
                                    }
                                    GlobalResponse::StreamDeleted { name: _ } => {
                                        // Need to get group_id from somewhere - maybe include in response
                                        None // TODO: Include group_id in response
                                    }
                                    GlobalResponse::GroupCreated { id } => {
                                        if let Some(group_info) = self.state.get_group(id).await {
                                            Some(Event::GroupCreated {
                                                group_id: *id,
                                                members: group_info.members.clone(),
                                            })
                                        } else {
                                            None
                                        }
                                    }
                                    GlobalResponse::GroupDissolved { id } => {
                                        Some(Event::GroupDeleted { group_id: *id })
                                    }
                                    _ => None,
                                };

                                if let Some(event) = event {
                                    let publisher = publisher.clone();
                                    let source = format!("global-consensus-{node_id}");

                                    // For critical events that affect routing, use synchronous handling
                                    match &event {
                                        Event::StreamCreated { .. }
                                        | Event::GroupCreated { .. } => {
                                            // Wait for routing service to update before continuing
                                            match publisher.request(event, source).await {
                                                Ok(EventResult::Success) => {
                                                    tracing::debug!("Routing updated successfully");
                                                }
                                                Ok(EventResult::Failed(msg)) => {
                                                    tracing::error!(
                                                        "Failed to update routing: {}",
                                                        msg
                                                    );
                                                    // Note: We log but don't fail the consensus operation
                                                    // as routing is eventually consistent
                                                }
                                                Ok(result) => {
                                                    tracing::debug!(
                                                        "Routing handler returned: {:?}",
                                                        result
                                                    );
                                                }
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Failed to notify routing service: {}",
                                                        e
                                                    );
                                                }
                                            }
                                        }
                                        _ => {
                                            // Other events can be async
                                            tokio::spawn(async move {
                                                if let Err(e) =
                                                    publisher.publish(event, source).await
                                                {
                                                    tracing::warn!(
                                                        "Failed to publish consensus event: {}",
                                                        e
                                                    );
                                                }
                                            });
                                        }
                                    }
                                }
                            }

                            responses.push(response)
                        }
                        Err(e) => responses.push(GlobalResponse::Error {
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
                    if let (Some(publisher), Some(node_id)) = (publisher.as_ref(), node_id.as_ref())
                    {
                        let event = Event::MembershipChanged {
                            new_members: membership
                                .nodes()
                                .map(|(node_id, _)| node_id.clone())
                                .collect(),
                            removed_members: vec![], // Would need to track previous membership
                        };

                        let publisher = publisher.clone();
                        let source = format!("global-consensus-{node_id}");
                        tokio::spawn(async move {
                            if let Err(e) = publisher.publish(event, source).await {
                                tracing::warn!("Failed to publish membership event: {}", e);
                            }
                        });
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
