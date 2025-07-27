//! Handler for GetNodeGroups command

use async_trait::async_trait;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::foundation::types::ConsensusGroupId;
use crate::services::group_consensus::commands::GetNodeGroups;
use crate::services::group_consensus::types::ConsensusLayers;
use proven_storage::StorageAdaptor;

/// Handler for GetNodeGroups command
pub struct GetNodeGroupsHandler<S>
where
    S: StorageAdaptor,
{
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
}

impl<S> GetNodeGroupsHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(groups: ConsensusLayers<S>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl<S> RequestHandler<GetNodeGroups> for GetNodeGroupsHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetNodeGroups,
        _metadata: EventMetadata,
    ) -> Result<Vec<ConsensusGroupId>, EventError> {
        let groups_guard = self.groups.read().await;
        Ok(groups_guard.keys().cloned().collect())
    }
}
