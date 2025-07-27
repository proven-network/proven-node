//! Group consensus event subscriber for stream service

use async_trait::async_trait;
use proven_attestation::Attestor;
use std::sync::Arc;
use tracing::debug;

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::services::group_consensus::events::GroupConsensusEvent;
use crate::services::stream::StreamService;
use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Subscriber for group consensus events that handles message persistence
#[derive(Clone)]
pub struct GroupConsensusSubscriber<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<T, G, A, S>>,
    local_node_id: NodeId,
}

impl<T, G, A, S> GroupConsensusSubscriber<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Create a new group consensus subscriber
    pub fn new(stream_service: Arc<StreamService<T, G, A, S>>, local_node_id: NodeId) -> Self {
        Self {
            stream_service,
            local_node_id,
        }
    }
}

#[async_trait]
impl<T, G, A, S> EventHandler<GroupConsensusEvent> for GroupConsensusSubscriber<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
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
