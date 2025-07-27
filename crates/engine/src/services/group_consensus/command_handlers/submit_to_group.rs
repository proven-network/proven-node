//! Handler for SubmitToGroup command

use async_trait::async_trait;

use crate::consensus::group::types::GroupResponse;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::SubmitToGroup;
use crate::services::group_consensus::types::ConsensusLayers;
use proven_storage::StorageAdaptor;

/// Handler for SubmitToGroup command
pub struct SubmitToGroupHandler<S>
where
    S: StorageAdaptor,
{
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
}

impl<S> SubmitToGroupHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(groups: ConsensusLayers<S>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl<S> RequestHandler<SubmitToGroup> for SubmitToGroupHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: SubmitToGroup,
        _metadata: EventMetadata,
    ) -> Result<GroupResponse, EventError> {
        let groups_guard = self.groups.read().await;
        if let Some(group) = groups_guard.get(&request.group_id) {
            match group.submit_request(request.request).await {
                Ok(response) => Ok(response),
                Err(e) => Err(EventError::Internal(e.to_string())),
            }
        } else {
            Err(EventError::Internal(format!(
                "Group {:?} not found",
                request.group_id
            )))
        }
    }
}
