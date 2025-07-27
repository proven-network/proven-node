//! Handler for GetGroupState command

use async_trait::async_trait;
use std::sync::Arc;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::GetGroupState;
use crate::services::group_consensus::{GroupConsensusService, GroupStateInfo};
use proven_attestation::Attestor;
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for GetGroupState command
pub struct GetGroupStateHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<GroupConsensusService<T, G, A, S>>,
}

impl<T, G, A, S> GetGroupStateHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GroupConsensusService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetGroupState> for GetGroupStateHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetGroupState,
        _metadata: EventMetadata,
    ) -> Result<Option<GroupStateInfo>, EventError> {
        self.service
            .get_group_state_info(request.group_id)
            .await
            .map(Some)
            .or_else(|_| Ok(None))
    }
}
