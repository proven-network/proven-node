//! Handler for EnsureStreamInitializedInGroup command

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::consensus::group::types::{AdminOperation, GroupRequest};
use crate::foundation::events::{Error as EventError, EventBus, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::{
    EnsureStreamInitializedInGroup, GetStreamState, SubmitToGroup,
};
use crate::services::group_consensus::types::ConsensusLayers;
use proven_storage::StorageAdaptor;
use proven_topology::NodeId;

/// Handler for EnsureStreamInitializedInGroup command
pub struct EnsureStreamInitializedInGroupHandler<S>
where
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
    /// Event bus for sending commands
    event_bus: Arc<EventBus>,
}

impl<S> EnsureStreamInitializedInGroupHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(node_id: NodeId, groups: ConsensusLayers<S>, event_bus: Arc<EventBus>) -> Self {
        Self {
            node_id,
            groups,
            event_bus,
        }
    }
}

#[async_trait]
impl<S> RequestHandler<EnsureStreamInitializedInGroup> for EnsureStreamInitializedInGroupHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: EnsureStreamInitializedInGroup,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "Ensuring stream {} is initialized in group {:?}",
            request.stream_name, request.group_id
        );

        // First, check if the stream is already initialized (idempotent)
        let check_state_cmd = GetStreamState {
            group_id: request.group_id,
            stream_name: request.stream_name.clone(),
        };

        match self.event_bus.request(check_state_cmd).await {
            Ok(Some(_)) => {
                // Stream already initialized - nothing to do
                debug!(
                    "Stream {} already initialized in group {:?}",
                    request.stream_name, request.group_id
                );
                return Ok(());
            }
            Ok(None) => {
                // Stream not initialized yet - proceed with initialization
                debug!(
                    "Stream {} not yet initialized in group {:?}, initializing now",
                    request.stream_name, request.group_id
                );
            }
            Err(e) => {
                // Error checking state - log but proceed with initialization attempt
                debug!(
                    "Error checking stream {} state in group {:?}: {}, attempting initialization",
                    request.stream_name, request.group_id, e
                );
            }
        }

        // Create the group request to initialize the stream
        let group_request = GroupRequest::Admin(AdminOperation::InitializeStream {
            stream_name: request.stream_name.clone(),
        });

        // Use SubmitToGroup which handles leader forwarding properly
        let submit_cmd = SubmitToGroup {
            group_id: request.group_id,
            request: group_request,
        };

        match self.event_bus.request(submit_cmd).await {
            Ok(_) => {
                info!(
                    "Successfully ensured stream {} is initialized in group {:?}",
                    request.stream_name, request.group_id
                );
                Ok(())
            }
            Err(e) => {
                debug!(
                    "Failed to ensure stream {} is initialized in group {:?}: {}",
                    request.stream_name, request.group_id, e
                );
                // For idempotency, we don't return an error if initialization fails
                // The stream might already be initialized by another node
                Ok(())
            }
        }
    }
}
