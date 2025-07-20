//! Group consensus callbacks implementation

use async_trait::async_trait;
use std::{num::NonZero, sync::Arc};

use crate::{
    consensus::group::callbacks::GroupConsensusCallbacks,
    consensus::group::state::GroupState,
    consensus::group::types::MessageData,
    error::ConsensusResult,
    foundation::types::ConsensusGroupId,
    services::event::{Event, EventPublisher},
    services::stream::{StreamName, StreamService},
};
use proven_storage::{LogStorage, StorageAdaptor};
use proven_topology::NodeId;

/// Implementation of group consensus callbacks
pub struct GroupConsensusCallbacksImpl<S: StorageAdaptor> {
    group_id: ConsensusGroupId,
    node_id: NodeId,
    event_publisher: Option<EventPublisher>,
    stream_service: Option<Arc<StreamService<S>>>,
}

impl<S: StorageAdaptor> GroupConsensusCallbacksImpl<S> {
    /// Create new callbacks implementation
    pub fn new(
        group_id: ConsensusGroupId,
        node_id: NodeId,
        event_publisher: Option<EventPublisher>,
        stream_service: Option<Arc<StreamService<S>>>,
    ) -> Self {
        Self {
            group_id,
            node_id,
            event_publisher,
            stream_service,
        }
    }
}

#[async_trait]
impl<S: StorageAdaptor + 'static> GroupConsensusCallbacks for GroupConsensusCallbacksImpl<S> {
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

    async fn on_messages_appended(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
        messages: &[(MessageData, NonZero<u64>, u64)], // (message, sequence, timestamp)
    ) -> ConsensusResult<()> {
        tracing::debug!(
            "Messages appended to stream {} in group {:?}, count: {}",
            stream_name,
            group_id,
            messages.len()
        );

        // Directly persist the messages to storage via stream service
        if let Some(ref stream_service) = self.stream_service {
            let stream_name = StreamName::new(stream_name);

            // Get or create the storage for this stream
            let _storage = stream_service.get_or_create_storage(&stream_name).await;

            // Prepare messages for batch append
            let mut entries = Vec::new();
            let mut total_bytes = 0u64;

            for (message, sequence, timestamp) in messages {
                let stored_message = crate::services::stream::StoredMessage {
                    sequence: *sequence,
                    data: message.clone(),
                    timestamp: *timestamp,
                };

                // Serialize the message
                let mut bytes = Vec::new();
                if let Err(e) = ciborium::into_writer(&stored_message, &mut bytes) {
                    tracing::error!(
                        "Failed to serialize message {} for stream {}: {}",
                        sequence,
                        stream_name,
                        e
                    );
                    continue;
                }

                total_bytes += message.payload.len() as u64;
                entries.push((*sequence, bytes.into()));
            }

            // Batch append to storage
            if !entries.is_empty() {
                let namespace =
                    proven_storage::StorageNamespace::new(format!("stream_{stream_name}"));
                if let Err(e) = stream_service
                    .storage_manager()
                    .stream_storage()
                    .append(&namespace, entries)
                    .await
                {
                    tracing::error!(
                        "Failed to persist {} messages to stream {} storage: {}",
                        messages.len(),
                        stream_name,
                        e
                    );
                } else {
                    // Update stream metadata
                    if let Some((_, _, last_timestamp)) = messages.last() {
                        stream_service
                            .update_stream_metadata_for_append(
                                &stream_name,
                                *last_timestamp,
                                total_bytes,
                            )
                            .await;
                    }

                    tracing::info!(
                        "Successfully persisted {} messages to stream {} storage (sequences: {:?})",
                        messages.len(),
                        stream_name,
                        messages.iter().map(|(_, seq, _)| seq).collect::<Vec<_>>()
                    );
                }
            }
        }

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
