//! Handler for RemoveNodeFromConsensus command

use std::sync::Arc;

use async_trait::async_trait;
use proven_storage::LogStorage;
use proven_topology::NodeId;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::RemoveNodeFromConsensus;

/// Handler for RemoveNodeFromConsensus command
pub struct RemoveNodeFromGroupHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> RemoveNodeFromGroupHandler<L>
where
    L: LogStorage,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<RemoveNodeFromConsensus> for RemoveNodeFromGroupHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: RemoveNodeFromConsensus,
        _metadata: EventMetadata,
    ) -> Result<Vec<NodeId>, EventError> {
        info!(
            "RemoveNodeFromConsensusHandler: Removing node {} from global consensus",
            request.node_id
        );

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Create the remove node request
        let global_request = GlobalRequest::RemoveNodeFromGroup {
            node_id: request.node_id,
        };

        // Submit the request
        let response = consensus
            .submit_request(global_request)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to remove node: {e}")))?;

        match response {
            GlobalResponse::NodeRemoved { node_id } => {
                info!("Node {} removed successfully", node_id);
                // Return empty vec as we don't have current members here
                Ok(vec![])
            }
            GlobalResponse::Error { message } => Err(EventError::Internal(format!(
                "Failed to remove node: {message}"
            ))),
            _ => Err(EventError::Internal(
                "Unexpected response type for RemoveNode".to_string(),
            )),
        }
    }
}
