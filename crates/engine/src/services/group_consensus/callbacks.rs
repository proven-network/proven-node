//! Group consensus callbacks implementation

use async_trait::async_trait;
use std::{num::NonZero, sync::Arc};

use crate::{
    consensus::group::callbacks::GroupConsensusCallbacks,
    error::ConsensusResult,
    foundation::{group_state::GroupState, types::ConsensusGroupId},
    services::{event::bus::EventBus, group_consensus::GroupConsensusEvent},
};
use proven_storage::StorageAdaptor;
use proven_topology::NodeId;

/// Implementation of group consensus callbacks
pub struct GroupConsensusCallbacksImpl<S: StorageAdaptor> {
    group_id: ConsensusGroupId,
    node_id: NodeId,
    event_bus: Option<Arc<EventBus>>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: StorageAdaptor> GroupConsensusCallbacksImpl<S> {
    /// Create new callbacks implementation
    pub fn new(
        group_id: ConsensusGroupId,
        node_id: NodeId,
        event_bus: Option<Arc<EventBus>>,
    ) -> Self {
        Self {
            group_id,
            node_id,
            event_bus,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<S: StorageAdaptor + 'static> GroupConsensusCallbacks for GroupConsensusCallbacksImpl<S> {
    async fn on_state_synchronized(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        tracing::info!(
            "Group {:?} state synchronized on node {}",
            group_id,
            self.node_id
        );

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GroupConsensusEvent::StateSynchronized { group_id };
            event_bus.publish(event).await;
        }

        Ok(())
    }

    async fn on_stream_created(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Stream {} created in group {:?} on node {}",
            stream_name,
            group_id,
            self.node_id
        );

        // Group consensus doesn't publish stream creation events
        // That's handled by global consensus

        Ok(())
    }

    async fn on_stream_removed(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Stream {} removed from group {:?} on node {}",
            stream_name,
            group_id,
            self.node_id
        );

        // Group consensus doesn't publish stream deletion events
        // That's handled by global consensus

        Ok(())
    }

    async fn on_messages_appended(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        entries: Arc<Vec<bytes::Bytes>>,
    ) -> ConsensusResult<()> {
        tracing::debug!(
            "Messages appended to stream {} in group {:?}, {} entries",
            stream_name,
            group_id,
            entries.len()
        );

        // Publish event with pre-serialized entries for persistence
        if let Some(ref event_bus) = self.event_bus {
            let event = GroupConsensusEvent::MessagesToPersist(Box::new(
                crate::services::group_consensus::events::MessagesToPersist {
                    stream_name: stream_name.to_string(),
                    entries: entries.clone(),
                },
            ));
            event_bus.publish(event).await;
        }

        // Also publish the metadata event for other subscribers
        if let Some(ref event_bus) = self.event_bus {
            let event = GroupConsensusEvent::MessagesAppended(Box::new(
                crate::services::group_consensus::events::MessagesAppendedData {
                    group_id,
                    stream_name: stream_name.to_string(),
                    message_count: entries.len(),
                },
            ));
            event_bus.publish(event).await;
        }

        // Storage is now handled by StreamService through the MessagesToPersist event

        Ok(())
    }

    async fn on_membership_changed(
        &self,
        group_id: ConsensusGroupId,
        added_members: &[NodeId],
        removed_members: &[NodeId],
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Group {:?} membership changed - added: {:?}, removed: {:?}",
            group_id,
            added_members,
            removed_members
        );

        // Publish event
        if let Some(ref event_bus) = self.event_bus {
            let event = GroupConsensusEvent::MembershipChanged(Box::new(
                crate::services::group_consensus::events::MembershipChangedData {
                    group_id,
                    added_members: added_members.to_vec(),
                    removed_members: removed_members.to_vec(),
                },
            ));
            event_bus.publish(event).await;
        }

        Ok(())
    }
}
