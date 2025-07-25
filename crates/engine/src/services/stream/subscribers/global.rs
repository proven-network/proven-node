//! Global consensus event subscriber for stream service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{EventHandler, EventMetadata};
use crate::foundation::types::ConsensusGroupId;
use crate::services::global_consensus::events::GlobalConsensusEvent;
use crate::services::stream::{StreamName, StreamService};
use proven_storage::StorageAdaptor;
use proven_topology::NodeId;

/// Subscriber for global consensus events that manages streams
#[derive(Clone)]
pub struct GlobalConsensusSubscriber<S>
where
    S: StorageAdaptor,
{
    stream_service: Arc<StreamService<S>>,
    local_node_id: NodeId,
}

impl<S> GlobalConsensusSubscriber<S>
where
    S: StorageAdaptor,
{
    /// Create a new global consensus subscriber
    pub fn new(stream_service: Arc<StreamService<S>>, local_node_id: NodeId) -> Self {
        Self {
            stream_service,
            local_node_id,
        }
    }
}

#[async_trait]
impl<S> EventHandler<GlobalConsensusEvent> for GlobalConsensusSubscriber<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(&self, event: GlobalConsensusEvent, _metadata: EventMetadata) {
        match event {
            GlobalConsensusEvent::StateSynchronized { snapshot } => {
                // Process all streams from the snapshot
                info!(
                    "StreamSubscriber: Processing state sync with {} streams",
                    snapshot.streams.len()
                );

                // First, figure out which groups this node is part of
                let my_groups: std::collections::HashSet<_> = snapshot
                    .groups
                    .iter()
                    .filter(|g| g.members.contains(&self.local_node_id))
                    .map(|g| g.group_id)
                    .collect();

                info!(
                    "StreamSubscriber: This node is member of {} groups",
                    my_groups.len()
                );

                // Create streams for groups we're a member of
                for stream in &snapshot.streams {
                    if my_groups.contains(&stream.group_id) {
                        info!(
                            "StreamSubscriber: Creating stream {} from snapshot (group {:?})",
                            stream.stream_name, stream.group_id
                        );

                        match self
                            .stream_service
                            .create_stream(
                                stream.stream_name.clone(),
                                stream.config.clone(),
                                stream.group_id,
                            )
                            .await
                        {
                            Ok(_) => {
                                info!(
                                    "Successfully created stream {} from snapshot",
                                    stream.stream_name
                                );
                            }
                            Err(e) if e.to_string().contains("already exists") => {
                                debug!("Stream {} already exists", stream.stream_name);
                            }
                            Err(e) => {
                                error!(
                                    "Failed to create stream {} from snapshot: {}",
                                    stream.stream_name, e
                                );
                            }
                        }
                    }
                }
            }

            GlobalConsensusEvent::StreamCreated {
                stream_name,
                config: _,
                group_id,
            } => {
                // Stream creation is now handled synchronously by the global consensus callback
                // using the command pattern. This event is just informational.
                info!(
                    "StreamSubscriber: Stream {} created in group {:?} (handled by command)",
                    stream_name, group_id
                );
            }

            GlobalConsensusEvent::StreamDeleted { stream_name } => {
                // Stream deletion is now handled synchronously by the global consensus callback
                // using the command pattern. This event is just informational.
                info!(
                    "StreamSubscriber: Stream {} deleted (handled by command)",
                    stream_name
                );
            }

            GlobalConsensusEvent::GroupCreated { .. } => {
                // Stream service doesn't need to handle group creation directly
                debug!("StreamSubscriber: Group created event (no action needed)");
            }

            GlobalConsensusEvent::GroupDissolved { group_id } => {
                // When a group is dissolved, we should clean up its streams
                info!(
                    "StreamSubscriber: Group {:?} dissolved, cleaning up streams",
                    group_id
                );

                // TODO: We might want to add a method to StreamService to delete all streams
                // belonging to a specific group
            }

            GlobalConsensusEvent::MembershipChanged { .. } => {
                // Membership changes might affect which streams we should have locally
                // but for now we'll rely on explicit stream creation/deletion events
                debug!("StreamSubscriber: Global membership changed");
            }

            GlobalConsensusEvent::LeaderChanged { .. } => {
                // Global leader changes don't directly affect streams
                debug!("StreamSubscriber: Global leader changed");
            }
        }
    }
}
