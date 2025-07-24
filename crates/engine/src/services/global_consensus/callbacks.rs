//! Global consensus callbacks implementation for the service

use std::sync::Arc;

use proven_topology::NodeId;

use crate::{
    consensus::global::GlobalConsensusCallbacks,
    error::ConsensusResult,
    foundation::{
        GlobalState, GlobalStateReader, GroupInfo, state_access::GlobalStateRead,
        types::ConsensusGroupId,
    },
    services::{
        event::bus::EventBus,
        global_consensus::{
            GlobalConsensusEvent,
            events::{GlobalStateSnapshot, GroupSnapshot, StreamSnapshot},
        },
        stream::StreamName,
    },
};

/// GlobalConsensusCallbacks implementation for GlobalConsensusService
pub struct GlobalConsensusCallbacksImpl {
    node_id: NodeId,
    event_bus: Option<Arc<EventBus>>,
    global_state: GlobalStateReader,
}

impl GlobalConsensusCallbacksImpl {
    /// Create new callbacks implementation
    pub fn new(
        node_id: NodeId,
        event_bus: Option<Arc<EventBus>>,
        global_state: GlobalStateReader,
    ) -> Self {
        Self {
            node_id,
            event_bus,
            global_state,
        }
    }
}

#[async_trait::async_trait]
impl GlobalConsensusCallbacks for GlobalConsensusCallbacksImpl {
    async fn on_state_synchronized(&self) -> ConsensusResult<()> {
        tracing::info!(
            "Global state synchronized - publishing snapshot for node {}",
            self.node_id
        );

        // Publish event with complete state snapshot
        if let Some(ref event_bus) = self.event_bus {
            // Build the snapshot
            let snapshot = {
                let all_groups = self.global_state.get_all_groups().await;
                let all_streams = self.global_state.get_all_streams().await;

                GlobalStateSnapshot {
                    groups: all_groups
                        .into_iter()
                        .map(|g| GroupSnapshot {
                            group_id: g.id,
                            members: g.members,
                        })
                        .collect(),
                    streams: all_streams
                        .into_iter()
                        .map(|s| StreamSnapshot {
                            stream_name: s.name,
                            config: s.config,
                            group_id: s.group_id,
                        })
                        .collect(),
                }
            }; // Lock is dropped here

            let event = GlobalConsensusEvent::StateSynchronized {
                snapshot: Box::new(snapshot),
            };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_group_created(
        &self,
        group_id: ConsensusGroupId,
        group_info: &GroupInfo,
    ) -> ConsensusResult<()> {
        // Just publish event - subscribers will handle all the work
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::GroupCreated {
                group_id,
                members: group_info.members.clone(),
            };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_group_dissolved(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        // TODO: Handle group dissolution
        tracing::info!("Group dissolved: {:?}", group_id);

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::GroupDissolved { group_id };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_stream_created(
        &self,
        stream_name: &StreamName,
        config: &crate::services::stream::StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Just publish event - subscribers will handle all the work
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::StreamCreated {
                stream_name: stream_name.clone(),
                config: config.clone(),
                group_id,
            };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_stream_deleted(&self, stream_name: &StreamName) -> ConsensusResult<()> {
        // Just publish event - subscribers will handle all the work
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::StreamDeleted {
                stream_name: stream_name.clone(),
            };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_membership_changed(
        &self,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Global consensus membership changed: added {:?}, removed {:?}",
            added_members,
            removed_members
        );

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GlobalConsensusEvent::MembershipChanged {
                added_members: added_members.to_vec(),
                removed_members: removed_members.to_vec(),
            };
            event_bus.publish(event).await;
        }

        Ok(())
    }
}
