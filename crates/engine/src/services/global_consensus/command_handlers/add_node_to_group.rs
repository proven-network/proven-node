//! Handler for adding nodes to consensus groups (not the global consensus itself)

use std::sync::Arc;

use async_trait::async_trait;
use proven_storage::LogStorage;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::AddNodeToGroup;

/// Handler for adding nodes to specific consensus groups
pub struct AddNodeToGroupHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> AddNodeToGroupHandler<L>
where
    L: LogStorage,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<AddNodeToGroup> for AddNodeToGroupHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: AddNodeToGroup,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "AddNodeToGroupHandler: Adding node {} to group {}",
            request.node_id, request.group_id
        );

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Create the add node request
        let global_request = GlobalRequest::AddNodeToGroup {
            node_id: request.node_id.clone(),
            metadata: Default::default(),
        };

        // Submit the request
        let response = consensus
            .submit_request(global_request)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to add node: {e}")))?;

        match response {
            GlobalResponse::NodeAdded { node_id } => {
                info!("Node {} added successfully to group", node_id);
                Ok(())
            }
            GlobalResponse::Error { message } => Err(EventError::Internal(format!(
                "Failed to add node to group: {message}"
            ))),
            _ => Err(EventError::Internal(
                "Unexpected response type for AddNodeToGroup".to_string(),
            )),
        }
    }
}
