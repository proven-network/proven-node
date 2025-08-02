//! Handler for CreateGroupStream command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::foundation::{
    events::{Error as EventError, EventBus, EventMetadata, RequestHandler},
    models::stream::StreamPlacement,
    routing::RoutingTable,
    types::ConsensusGroupId,
};
use crate::services::global_consensus::commands::{CreateGroupStream, SubmitGlobalRequest};
use proven_storage::LogStorage;

/// Handler for CreateGroupStream command
pub struct CreateGroupStreamHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
    routing_table: Arc<RoutingTable>,
    event_bus: Arc<EventBus>,
}

impl<L> CreateGroupStreamHandler<L>
where
    L: LogStorage,
{
    pub fn new(
        consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
        routing_table: Arc<RoutingTable>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            consensus_layer,
            routing_table,
            event_bus,
        }
    }
}

#[async_trait]
impl<L> RequestHandler<CreateGroupStream> for CreateGroupStreamHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: CreateGroupStream,
        _metadata: EventMetadata,
    ) -> Result<ConsensusGroupId, EventError> {
        info!(
            "CreateGroupStreamHandler: Creating stream '{}' in group",
            request.stream_name
        );

        // Determine which group to create the stream in
        let group_id = if let Some(group) = request.target_group {
            // Use the specified group
            group
        } else {
            // Find the least loaded group
            self.routing_table
                .find_least_loaded_group()
                .await
                .ok_or_else(|| {
                    EventError::Internal("No available groups for stream creation".to_string())
                })?
        };

        debug!(
            "Selected group {:?} for stream '{}'",
            group_id, request.stream_name
        );

        // Check if consensus layer is initialized
        {
            let consensus_guard = self.consensus_layer.read().await;
            if consensus_guard.is_none() {
                return Err(EventError::Internal(
                    "Global consensus not initialized".to_string(),
                ));
            }
        }

        // Submit the request to global consensus with group placement
        let global_request = GlobalRequest::CreateStream {
            stream_name: request.stream_name.clone(),
            config: request.config,
            placement: StreamPlacement::Group(group_id),
        };

        // Use SubmitGlobalRequest which handles leader forwarding
        let submit_request = SubmitGlobalRequest {
            request: global_request,
        };

        match self.event_bus.request(submit_request).await {
            Ok(GlobalResponse::StreamCreated {
                stream_name: _,
                placement,
            }) => {
                match placement {
                    StreamPlacement::Group(actual_group_id) => {
                        info!(
                            "Stream '{}' created successfully in group {:?}",
                            request.stream_name, actual_group_id
                        );
                        Ok(actual_group_id)
                    }
                    StreamPlacement::Global => {
                        // This shouldn't happen for CreateGroupStream
                        Err(EventError::Internal(
                            "Stream was created as global instead of in a group".to_string(),
                        ))
                    }
                }
            }
            Ok(GlobalResponse::StreamAlreadyExists {
                stream_name: _,
                placement,
            }) => match placement {
                StreamPlacement::Group(existing_group_id) => {
                    info!(
                        "Stream '{}' already exists in group {:?}",
                        request.stream_name, existing_group_id
                    );
                    Ok(existing_group_id)
                }
                StreamPlacement::Global => Err(EventError::Internal(
                    "Stream already exists as a global stream".to_string(),
                )),
            },
            Ok(GlobalResponse::Error { message }) => Err(EventError::Internal(format!(
                "Failed to create stream: {message}"
            ))),
            Ok(_) => Err(EventError::Internal(
                "Unexpected response from global consensus".to_string(),
            )),
            Err(e) => Err(EventError::Internal(format!(
                "Failed to create stream: {e}"
            ))),
        }
    }
}
