//! Handler for CreateGlobalStream command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::foundation::{
    StreamInfo,
    events::{Error as EventError, EventBus, EventMetadata, RequestHandler},
    models::stream::StreamPlacement,
};
use crate::services::global_consensus::commands::{CreateGlobalStream, SubmitGlobalRequest};
use proven_storage::LogStorage;

/// Handler for CreateGlobalStream command
pub struct CreateGlobalStreamHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
    event_bus: Arc<EventBus>,
}

impl<L> CreateGlobalStreamHandler<L>
where
    L: LogStorage,
{
    pub fn new(
        consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            consensus_layer,
            event_bus,
        }
    }
}

#[async_trait]
impl<L> RequestHandler<CreateGlobalStream> for CreateGlobalStreamHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: CreateGlobalStream,
        _metadata: EventMetadata,
    ) -> Result<StreamInfo, EventError> {
        info!(
            "CreateGlobalStreamHandler: Creating global stream '{}'",
            request.stream_name
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

        // Submit the request to global consensus with global placement
        let global_request = GlobalRequest::CreateStream {
            stream_name: request.stream_name.clone(),
            config: request.config.clone(),
            placement: StreamPlacement::Global,
        };

        // Use SubmitGlobalRequest which handles leader forwarding
        let submit_request = SubmitGlobalRequest {
            request: global_request,
        };

        match self.event_bus.request(submit_request).await {
            Ok(GlobalResponse::StreamCreated {
                stream_name,
                placement,
            }) => {
                match placement {
                    StreamPlacement::Global => {
                        let stream_info = StreamInfo {
                            stream_name: stream_name.clone(),
                            config: request.config,
                            placement: StreamPlacement::Global,
                            created_at: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        };
                        info!(
                            "Global stream '{}' created successfully",
                            request.stream_name
                        );
                        Ok(stream_info)
                    }
                    StreamPlacement::Group(_) => {
                        // This shouldn't happen for CreateGlobalStream
                        Err(EventError::Internal(
                            "Stream was created in a group instead of as global".to_string(),
                        ))
                    }
                }
            }
            Ok(GlobalResponse::StreamAlreadyExists {
                stream_name,
                placement,
            }) => {
                match placement {
                    StreamPlacement::Global => {
                        // Return the existing stream info
                        let stream_info = StreamInfo {
                            stream_name: stream_name.clone(),
                            config: request.config,
                            placement: StreamPlacement::Global,
                            created_at: 0, // We don't know the actual creation time
                        };
                        info!("Global stream '{}' already exists", request.stream_name);
                        Ok(stream_info)
                    }
                    StreamPlacement::Group(_) => Err(EventError::Internal(
                        "Stream already exists as a group stream".to_string(),
                    )),
                }
            }
            Ok(GlobalResponse::Error { message }) => Err(EventError::Internal(format!(
                "Failed to create global stream: {message}"
            ))),
            Ok(_) => Err(EventError::Internal(
                "Unexpected response from global consensus".to_string(),
            )),
            Err(e) => Err(EventError::Internal(format!(
                "Failed to create global stream: {e}"
            ))),
        }
    }
}
