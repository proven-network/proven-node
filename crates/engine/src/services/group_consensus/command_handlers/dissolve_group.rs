//! Handler for DissolveGroup command

use async_trait::async_trait;
use tracing::info;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::DissolveGroup;
use crate::services::group_consensus::types::ConsensusLayers;
use proven_storage::StorageAdaptor;

/// Handler for DissolveGroup command
pub struct DissolveGroupHandler<S>
where
    S: StorageAdaptor,
{
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
}

impl<S> DissolveGroupHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(groups: ConsensusLayers<S>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl<S> RequestHandler<DissolveGroup> for DissolveGroupHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: DissolveGroup,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "DissolveGroupHandler: Group {:?} dissolved - removing local instance",
            request.group_id
        );

        // Remove the group from our local map
        let mut groups_guard = self.groups.write().await;
        if let Some(group) = groups_guard.remove(&request.group_id) {
            // Shutdown the consensus layer
            if let Err(e) = group.shutdown().await {
                tracing::error!(
                    "Failed to shutdown group {:?} consensus layer: {}",
                    request.group_id,
                    e
                );
            }
            info!("Successfully removed local group {:?}", request.group_id);
        } else {
            info!("Group {:?} not found locally", request.group_id);
        }

        Ok(())
    }
}
