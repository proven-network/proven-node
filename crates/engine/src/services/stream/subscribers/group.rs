//! Group consensus event subscriber for stream service

use async_trait::async_trait;
use std::num::NonZero;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::foundation::types::ConsensusGroupId;
use crate::services::group_consensus::events::GroupConsensusEvent;
use crate::services::stream::{StoredMessage, StreamName, StreamService};
use proven_storage::{LogStorage, StorageAdaptor};
use proven_topology::NodeId;

/// Subscriber for group consensus events that handles message persistence
#[derive(Clone)]
pub struct GroupConsensusSubscriber<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
    local_node_id: NodeId,
}

impl<S> GroupConsensusSubscriber<S>
where
    S: StorageAdaptor,
{
    /// Create a new group consensus subscriber
    pub fn new(stream_service: Arc<StreamService<S>>, local_node_id: NodeId) -> Self {
        Self {
            stream_service,
            local_node_id,
        }
    }
}

#[async_trait]
impl<S> EventHandler<GroupConsensusEvent> for GroupConsensusSubscriber<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(&self, event: GroupConsensusEvent, _metadata: EventMetadata) {
        match event {
            GroupConsensusEvent::MessagesAppended(data) => {
                let group_id = data.group_id;
                let stream_name = data.stream_name;
                let message_count = data.message_count;
                debug!(
                    "StreamSubscriber: {} messages appended to stream {} in group {:?}",
                    message_count, stream_name, group_id
                );

                // Note: The actual message persistence is handled by the group consensus
                // callbacks directly since they have access to the message data.
                // This event is informational for other services that need to know
                // messages were appended but don't need the actual data.

                // Note: Stream metadata is updated when messages are persisted
                // through the MessagesToPersist event
            }

            GroupConsensusEvent::StreamCreated {
                group_id,
                stream_name,
            } => {
                // This is emitted by group consensus when a stream is initialized
                // The actual stream creation is triggered by GlobalConsensusEvent
                debug!(
                    "StreamSubscriber: Stream {} initialized in group {:?}",
                    stream_name, group_id
                );
            }

            GroupConsensusEvent::StreamRemoved {
                group_id,
                stream_name,
            } => {
                // Stream removal at the group level
                debug!(
                    "StreamSubscriber: Stream {} removed from group {:?}",
                    stream_name, group_id
                );
            }

            GroupConsensusEvent::StateSynchronized { group_id } => {
                debug!("StreamSubscriber: Group {:?} state synchronized", group_id);
            }

            GroupConsensusEvent::MembershipChanged(data) => {
                let group_id = data.group_id;
                // Group membership changes might affect stream availability
                debug!("StreamSubscriber: Group {:?} membership changed", group_id);
            }

            GroupConsensusEvent::LeaderChanged {
                group_id,
                new_leader,
                ..
            } => {
                // Leader changes might affect write availability
                debug!(
                    "StreamSubscriber: Group {:?} leader changed to {:?}",
                    group_id, new_leader
                );
            }
        }
    }
}
