//! Groups consensus layer management
//!
//! This module handles consensus groups and stream operations within those groups.

use super::events::{ConsensusEvent, EventBus, EventResult, EventSubscriber};
use crate::{
    ConsensusGroupId, NodeId,
    core::group::{GroupStreamOperation, GroupsManager},
    error::ConsensusResult,
    operations::handlers::GroupStreamOperationResponse,
};

use std::{collections::HashMap, sync::Arc};

use proven_governance::Governance;
use proven_transport::Transport;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Groups consensus layer manager
pub struct GroupsConsensusLayer<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Node ID
    node_id: NodeId,
    /// Groups manager
    groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
    /// Event bus for receiving commands
    event_bus: Arc<EventBus>,
    /// Event subscriber handle
    event_subscriber: Option<EventSubscriber>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<T, G> GroupsConsensusLayer<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new groups consensus layer
    pub fn new(
        node_id: NodeId,
        groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            groups_manager,
            event_bus,
            event_subscriber: None,
            task_handle: None,
        }
    }

    /// Start the event processing loop
    pub async fn start(&mut self) -> ConsensusResult<()> {
        info!("Starting groups consensus layer for node {}", self.node_id);

        // Subscribe to events
        let mut subscriber = self
            .event_bus
            .subscribe(format!("groups-layer-{}", self.node_id))
            .await;

        // Clone what we need for the task
        let groups_manager = self.groups_manager.clone();
        let node_id = self.node_id.clone();

        // Start event processing task
        let handle = tokio::spawn(async move {
            info!("Groups layer event processor started");

            while let Some(event) = subscriber.recv().await {
                debug!("Groups layer received event: {:?}", event);

                match event {
                    ConsensusEvent::StreamCreated {
                        name,
                        config,
                        group_id,
                        reply_to,
                    } => {
                        let result =
                            Self::handle_stream_created(&groups_manager, name, config, group_id)
                                .await;

                        let _ = reply_to.reply(result);
                    }

                    ConsensusEvent::StreamDeleted {
                        name,
                        group_id,
                        reply_to,
                    } => {
                        let result =
                            Self::handle_stream_deleted(&groups_manager, name, group_id).await;

                        let _ = reply_to.reply(result);
                    }

                    ConsensusEvent::GroupCreated {
                        group_id,
                        members,
                        reply_to,
                    } => {
                        let result = Self::handle_group_created(
                            &groups_manager,
                            &node_id,
                            group_id,
                            members,
                        )
                        .await;

                        let _ = reply_to.reply(result);
                    }

                    ConsensusEvent::GroupDeleted { group_id, reply_to } => {
                        let result = Self::handle_group_deleted(&groups_manager, group_id).await;

                        let _ = reply_to.reply(result);
                    }

                    _ => {
                        // Other events not handled by groups layer
                        debug!("Groups layer ignoring event type");
                    }
                }
            }

            warn!("Groups layer event processor stopped");
        });

        self.task_handle = Some(handle);
        Ok(())
    }

    /// Stop the event processing loop
    pub async fn stop(&mut self) -> ConsensusResult<()> {
        info!("Stopping groups consensus layer");

        if let Some(handle) = self.task_handle.take() {
            handle.abort();
            let _ = handle.await;
        }

        Ok(())
    }

    /// Handle stream created event
    async fn handle_stream_created(
        groups_manager: &Arc<RwLock<GroupsManager<T, G>>>,
        name: String,
        config: crate::core::global::StreamConfig,
        group_id: ConsensusGroupId,
    ) -> EventResult {
        info!(
            "Groups layer handling stream creation: {} in group {:?}",
            name, group_id
        );

        let groups = groups_manager.read().await;

        // Release the read lock first
        drop(groups);

        // Check if we have this group
        match groups_manager.read().await.get_group(group_id).await {
            Ok(group) => {
                // Initialize stream in the local state machine
                match group
                    ._local_state
                    .write()
                    .await
                    .create_stream(&name, config)
                    .await
                {
                    Ok(()) => {
                        info!("Successfully initialized stream {} in groups layer", name);
                        EventResult::Success
                    }
                    Err(e) => {
                        warn!(
                            "Failed to initialize stream {} in groups layer: {}",
                            name, e
                        );
                        EventResult::Failed(e.to_string())
                    }
                }
            }
            Err(_) => {
                debug!(
                    "Group {:?} not found on this node for stream {}",
                    group_id, name
                );
                // This is OK - not all nodes have all groups
                EventResult::Success
            }
        }
    }

    /// Handle stream deleted event
    async fn handle_stream_deleted(
        groups_manager: &Arc<RwLock<GroupsManager<T, G>>>,
        name: String,
        group_id: ConsensusGroupId,
    ) -> EventResult {
        info!(
            "Groups layer handling stream deletion: {} from group {:?}",
            name, group_id
        );

        // Get the specific group
        let group = match groups_manager.read().await.get_group(group_id).await {
            Ok(g) => g,
            Err(e) => {
                warn!("Group {group_id:?} not found: {e}");
                return EventResult::Failed(format!("Group {group_id:?} not found"));
            }
        };

        // Check if stream exists in this group
        let streams = group._local_state.read().await.list_streams().await;
        if streams.contains(&name) {
            match group._local_state.write().await.remove_stream(&name).await {
                Ok(()) => {
                    info!(
                        "Successfully removed stream {} from group {:?}",
                        name, group_id
                    );
                    EventResult::Success
                }
                Err(e) => {
                    warn!(
                        "Failed to remove stream {} from group {:?}: {}",
                        name, group_id, e
                    );
                    EventResult::Failed(e.to_string())
                }
            }
        } else {
            // Stream not found in this group - that's OK
            debug!("Stream {} not found in group {:?}", name, group_id);
            EventResult::Success
        }
    }

    /// Handle group created event
    async fn handle_group_created(
        groups_manager: &Arc<RwLock<GroupsManager<T, G>>>,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> EventResult {
        info!("Groups layer handling group creation: {:?}", group_id);

        // Check if we're a member of this group
        if !members.contains(node_id) {
            debug!("Not a member of group {:?}, skipping", group_id);
            return EventResult::Success;
        }

        // Create the group
        match groups_manager
            .write()
            .await
            .create_group(group_id, members)
            .await
        {
            Ok(()) => {
                info!("Successfully created group {:?} in groups layer", group_id);
                EventResult::Success
            }
            Err(e) => {
                warn!(
                    "Failed to create group {:?} in groups layer: {}",
                    group_id, e
                );
                EventResult::Failed(e.to_string())
            }
        }
    }

    /// Handle group deleted event
    async fn handle_group_deleted(
        groups_manager: &Arc<RwLock<GroupsManager<T, G>>>,
        group_id: ConsensusGroupId,
    ) -> EventResult {
        info!("Groups layer handling group deletion: {:?}", group_id);

        match groups_manager.write().await.remove_group(group_id).await {
            Ok(()) => {
                info!(
                    "Successfully removed group {:?} from groups layer",
                    group_id
                );
                EventResult::Success
            }
            Err(e) => {
                warn!(
                    "Failed to remove group {:?} from groups layer: {}",
                    group_id, e
                );
                EventResult::Failed(e.to_string())
            }
        }
    }

    /// Process a local operation for a specific group
    pub async fn process_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        debug!(
            "Processing local operation for group {:?}: {:?}",
            group_id, operation
        );

        self.groups_manager
            .read()
            .await
            .process_operation(group_id, operation)
            .await
    }

    /// Get groups manager reference
    pub fn groups_manager(&self) -> Arc<RwLock<GroupsManager<T, G>>> {
        self.groups_manager.clone()
    }

    /// Check if a group exists
    pub async fn has_group(&self, group_id: ConsensusGroupId) -> bool {
        self.groups_manager.read().await.has_group(group_id).await
    }

    /// Get all groups this node is part of
    pub async fn get_my_groups(&self) -> HashMap<ConsensusGroupId, Vec<NodeId>> {
        let group_ids = self.groups_manager.read().await.get_all_group_ids().await;
        let mut result = HashMap::new();

        for group_id in group_ids {
            if let Ok(group) = self.groups_manager.read().await.get_group(group_id).await {
                // Get members from Raft metrics
                let metrics = group.raft.metrics().borrow().clone();
                let members: Vec<NodeId> = metrics
                    .membership_config
                    .membership()
                    .nodes()
                    .map(|(node_id, _)| node_id.clone())
                    .collect();
                result.insert(group_id, members);
            }
        }

        result
    }
}
