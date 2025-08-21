//! Handler for UpdateGlobalMembership command

use std::sync::Arc;

use async_trait::async_trait;
use proven_storage::LogStorage;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::UpdateGlobalMembership;

/// Handler for UpdateGlobalMembership command
pub struct UpdateGlobalMembershipHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> UpdateGlobalMembershipHandler<L>
where
    L: LogStorage,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<UpdateGlobalMembership> for UpdateGlobalMembershipHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: UpdateGlobalMembership,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "UpdateGlobalMembershipHandler: Updating membership - adding {:?}, removing {:?}",
            request.add_members, request.remove_members
        );

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Process additions
        for node_id in request.add_members {
            let add_request = GlobalRequest::AddNodeToGroup {
                node_id,
                metadata: Default::default(),
            };

            match consensus.submit_request(add_request).await {
                Ok(GlobalResponse::NodeAdded { node_id: _ }) => {
                    info!("Node {} added successfully", node_id);
                }
                Ok(GlobalResponse::Error { message }) => {
                    return Err(EventError::Internal(format!(
                        "Failed to add node {node_id}: {message}"
                    )));
                }
                Ok(_) => {
                    return Err(EventError::Internal(
                        "Unexpected response type for AddNode".to_string(),
                    ));
                }
                Err(e) => {
                    return Err(EventError::Internal(format!(
                        "Failed to add node {node_id}: {e}"
                    )));
                }
            }
        }

        // Process removals
        for node_id in request.remove_members {
            let remove_request = GlobalRequest::RemoveNodeFromGroup { node_id };

            match consensus.submit_request(remove_request).await {
                Ok(GlobalResponse::NodeRemoved { node_id: _ }) => {
                    info!("Node {} removed successfully", node_id);
                }
                Ok(GlobalResponse::Error { message }) => {
                    return Err(EventError::Internal(format!(
                        "Failed to remove node {node_id}: {message}"
                    )));
                }
                Ok(_) => {
                    return Err(EventError::Internal(
                        "Unexpected response type for RemoveNode".to_string(),
                    ));
                }
                Err(e) => {
                    return Err(EventError::Internal(format!(
                        "Failed to remove node {node_id}: {e}"
                    )));
                }
            }
        }

        info!("Membership update complete");
        Ok(())
    }
}
