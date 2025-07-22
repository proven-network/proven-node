//! Group consensus event subscriber for stream service

use async_trait::async_trait;
use std::num::NonZero;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{EventHandler, EventPriority};
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
    fn priority(&self) -> EventPriority {
        // Handle GroupConsensusEvents synchronously for message persistence
        EventPriority::Critical
    }

    async fn handle(&self, event: GroupConsensusEvent) {
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

            GroupConsensusEvent::MessagesToPersist(data) => {
                // Handle message persistence with zero-copy Arc'd data
                let stream_name = StreamName::new(&data.stream_name);
                let message_count = data.entries.len();

                // Get or create the storage for this stream
                let _storage = self
                    .stream_service
                    .get_or_create_storage(&stream_name)
                    .await;

                // Extract the pre-serialized entries
                let entries = data.into_bytes();

                // Batch append to storage
                if !entries.is_empty() {
                    let namespace =
                        proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));

                    match self
                        .stream_service
                        .storage_manager()
                        .stream_storage()
                        .append(&namespace, entries)
                        .await
                    {
                        Ok(last_seq) => {
                            // Stream metadata is updated by group consensus callbacks

                            info!(
                                "Successfully persisted {} messages to stream {} storage (last_seq: {})",
                                message_count, stream_name, last_seq
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to persist {} messages to stream {} storage: {}",
                                message_count, stream_name, e
                            );
                        }
                    }
                }
            }
        }
    }
}
