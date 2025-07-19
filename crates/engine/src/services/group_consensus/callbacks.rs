//! Group consensus callbacks implementation

use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    consensus::group::callbacks::GroupConsensusCallbacks,
    consensus::group::state::GroupState,
    consensus::group::types::MessageData,
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::event::{Event, EventPublisher},
};
use proven_topology::NodeId;

/// Implementation of group consensus callbacks
pub struct GroupConsensusCallbacksImpl {
    group_id: ConsensusGroupId,
    node_id: NodeId,
    event_publisher: Option<EventPublisher>,
}

impl GroupConsensusCallbacksImpl {
    /// Create new callbacks implementation
    pub fn new(
        group_id: ConsensusGroupId,
        node_id: NodeId,
        event_publisher: Option<EventPublisher>,
    ) -> Self {
        Self {
            group_id,
            node_id,
            event_publisher,
        }
    }
}

#[async_trait]
impl GroupConsensusCallbacks for GroupConsensusCallbacksImpl {
    async fn on_state_synchronized(
        &self,
        group_id: ConsensusGroupId,
        _state: &GroupState,
    ) -> ConsensusResult<()> {
        tracing::info!(
            "Group {:?} state synchronized on node {}",
            group_id,
            self.node_id
        );
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

    async fn on_message_appended(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        _message: &MessageData,
    ) -> ConsensusResult<()> {
        tracing::debug!(
            "Message appended to stream {} in group {:?}",
            stream_name,
            group_id
        );
        // We might want to publish message events in the future
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

        // Could publish membership change events if needed
        Ok(())
    }
}
