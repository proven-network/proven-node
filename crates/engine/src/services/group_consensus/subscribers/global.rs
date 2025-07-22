//! Global consensus event subscriber for group consensus service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{EventHandler, EventPriority};
use crate::services::global_consensus::events::GlobalConsensusEvent;
use crate::services::group_consensus::GroupConsensusService;
use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Subscriber for global consensus events that manages group consensus instances
#[derive(Clone)]
pub struct GlobalConsensusSubscriber<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
    local_node_id: NodeId,
}

impl<T, G, S> GlobalConsensusSubscriber<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Create a new global consensus subscriber
    pub fn new(
        group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            group_consensus_service,
            local_node_id,
        }
    }
}

#[async_trait]
impl<T, G, S> EventHandler<GlobalConsensusEvent> for GlobalConsensusSubscriber<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    fn priority(&self) -> EventPriority {
        // Handle GlobalConsensusEvents synchronously for group management
        EventPriority::Critical
    }

    async fn handle(&self, event: GlobalConsensusEvent) {
        match event {
            GlobalConsensusEvent::StateSynchronized { snapshot } => {
                // Process all groups where this node is a member
                info!(
                    "GroupConsensusSubscriber: Processing state sync with {} groups",
                    snapshot.groups.len()
                );

                for group in &snapshot.groups {
                    if group.members.contains(&self.local_node_id) {
                        info!(
                            "GroupConsensusSubscriber: Creating local group {:?} from snapshot",
                            group.group_id
                        );

                        if let Err(e) = self
                            .group_consensus_service
                            .create_group(group.group_id, group.members.clone())
                            .await
                        {
                            error!(
                                "Failed to create local group {:?} from snapshot: {}",
                                group.group_id, e
                            );
                        }
                    }
                }

                // Process streams after groups are created
                for stream in &snapshot.streams {
                    if let Ok(groups) = self.group_consensus_service.get_node_groups().await
                        && groups.contains(&stream.group_id)
                    {
                        info!(
                            "GroupConsensusSubscriber: Initializing stream {} in group {:?} from snapshot",
                            stream.stream_name, stream.group_id
                        );

                        let request = crate::consensus::group::GroupRequest::Admin(
                            crate::consensus::group::types::AdminOperation::InitializeStream {
                                stream: stream.stream_name.clone(),
                            },
                        );

                        match self
                            .group_consensus_service
                            .submit_to_group(stream.group_id, request)
                            .await
                        {
                            Ok(_) => {
                                debug!(
                                    "Successfully initialized stream {} from snapshot",
                                    stream.stream_name
                                );
                            }
                            Err(e) if e.is_not_leader() => {
                                debug!(
                                    "Not the leader for group {:?}, stream {} will be initialized by leader",
                                    stream.group_id, stream.stream_name
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Failed to initialize stream {} in group {:?}: {}",
                                    stream.stream_name, stream.group_id, e
                                );
                            }
                        }
                    }
                }
            }

            GlobalConsensusEvent::GroupCreated { group_id, members } => {
                // Check if this node is a member of the group
                if members.contains(&self.local_node_id) {
                    info!(
                        "GroupConsensusSubscriber: Creating local group {:?} with members: {:?}",
                        group_id, members
                    );

                    if let Err(e) = self
                        .group_consensus_service
                        .create_group(group_id, members.clone())
                        .await
                    {
                        error!("Failed to create local group {:?}: {}", group_id, e);
                    } else {
                        info!("Successfully created local group {:?}", group_id);
                    }
                } else {
                    debug!(
                        "GroupConsensusSubscriber: Not a member of group {:?}, skipping creation",
                        group_id
                    );
                }
            }

            GlobalConsensusEvent::GroupDissolved { group_id } => {
                // Check if we have this group locally by getting node groups
                if let Ok(groups) = self.group_consensus_service.get_node_groups().await
                    && groups.contains(&group_id)
                {
                    info!(
                        "GroupConsensusSubscriber: Group {:?} dissolved - would remove local instance",
                        group_id
                    );

                    // TODO: Add dissolve_group method to GroupConsensusService
                    // For now, just log that we would dissolve it
                }
            }

            GlobalConsensusEvent::StreamCreated {
                stream_name,
                config: _,
                group_id,
            } => {
                // Check if we're a member of the group that owns this stream
                if let Ok(groups) = self.group_consensus_service.get_node_groups().await
                    && groups.contains(&group_id)
                {
                    info!(
                        "GroupConsensusSubscriber: Initializing stream {} in local group {:?}",
                        stream_name, group_id
                    );

                    // Initialize the stream in group consensus
                    let request = crate::consensus::group::GroupRequest::Admin(
                        crate::consensus::group::types::AdminOperation::InitializeStream {
                            stream: stream_name.clone(),
                        },
                    );

                    match self
                        .group_consensus_service
                        .submit_to_group(group_id, request)
                        .await
                    {
                        Ok(_) => {
                            debug!(
                                "Successfully initialized stream {} in group {:?}",
                                stream_name, group_id
                            );
                        }
                        Err(e) if e.is_not_leader() => {
                            // Not the leader - this is expected and fine
                            debug!(
                                "Not the leader for group {:?}, stream {} will be initialized by leader",
                                group_id, stream_name
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to initialize stream {} in group {:?}: {}",
                                stream_name, group_id, e
                            );
                        }
                    }
                }
            }

            GlobalConsensusEvent::StreamDeleted { stream_name } => {
                // Stream deletion is handled at the global level
                debug!(
                    "GroupConsensusSubscriber: Stream {} deleted globally",
                    stream_name
                );
            }

            GlobalConsensusEvent::MembershipChanged { .. } => {
                // Global membership changes don't directly affect group consensus
                debug!("GroupConsensusSubscriber: Global membership changed");
            }

            GlobalConsensusEvent::LeaderChanged { .. } => {
                // Global leader changes don't directly affect group consensus
                debug!("GroupConsensusSubscriber: Global leader changed");
            }
        }
    }
}
