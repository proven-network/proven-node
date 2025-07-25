//! Event handlers for group consensus events (non-critical)

use std::sync::Arc;
use tracing::{debug, info};

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::services::group_consensus::events::GroupConsensusEvent;
use crate::services::stream::StreamService;
use proven_storage::StorageAdaptor;

/// Handler for non-critical group consensus events
#[derive(Clone)]
pub struct GroupConsensusEventHandler<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
}

impl<S> GroupConsensusEventHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(stream_service: Arc<StreamService<S>>) -> Self {
        Self { stream_service }
    }
}

#[async_trait::async_trait]
impl<S> EventHandler<GroupConsensusEvent> for GroupConsensusEventHandler<S>
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
                    "StreamEventHandler: {} messages appended to stream {} in group {:?}",
                    message_count, stream_name, group_id
                );

                // This is informational only - actual persistence is handled by PersistMessages command
            }

            GroupConsensusEvent::StreamCreated {
                group_id,
                stream_name,
            } => {
                // This is emitted by group consensus when a stream is initialized
                // The actual stream creation is triggered by GlobalConsensusEvent
                debug!(
                    "StreamEventHandler: Stream {} initialized in group {:?}",
                    stream_name, group_id
                );
            }

            GroupConsensusEvent::StreamRemoved {
                group_id,
                stream_name,
            } => {
                // Stream removal at the group level
                debug!(
                    "StreamEventHandler: Stream {} removed from group {:?}",
                    stream_name, group_id
                );
            }

            GroupConsensusEvent::StateSynchronized { group_id } => {
                debug!(
                    "StreamEventHandler: Group {:?} state synchronized",
                    group_id
                );
            }

            GroupConsensusEvent::MembershipChanged(data) => {
                let group_id = data.group_id;
                // Group membership changes might affect stream availability
                debug!(
                    "StreamEventHandler: Group {:?} membership changed",
                    group_id
                );
            }

            GroupConsensusEvent::LeaderChanged {
                group_id,
                new_leader,
                ..
            } => {
                // Leader changes might affect write availability
                debug!(
                    "StreamEventHandler: Group {:?} leader changed to {:?}",
                    group_id, new_leader
                );
            }
        }
    }
}
