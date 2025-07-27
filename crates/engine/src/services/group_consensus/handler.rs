//! Network service handler for group consensus

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};
use proven_storage::ConsensusStorage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
use crate::consensus::group::GroupConsensusLayer;
use crate::consensus::group::raft::GroupRaftMessageHandler;
use crate::foundation::types::ConsensusGroupId;

/// Type alias for consensus layers
type ConsensusLayers<S> =
    Arc<RwLock<HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<ConsensusStorage<S>>>>>>;

/// Group consensus service handler
pub struct GroupConsensusHandler<S>
where
    S: proven_storage::StorageAdaptor + 'static,
{
    groups: ConsensusLayers<S>,
}

impl<S> GroupConsensusHandler<S>
where
    S: proven_storage::StorageAdaptor + 'static,
{
    /// Create a new handler
    pub fn new(groups: ConsensusLayers<S>) -> Self {
        Self { groups }
    }
}

#[async_trait]
impl<S> Service for GroupConsensusHandler<S>
where
    S: proven_storage::StorageAdaptor + Send + Sync + 'static,
{
    type Request = GroupConsensusMessage;

    async fn handle(
        &self,
        request: Self::Request,
        _ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        match request {
            GroupConsensusMessage::Vote { group_id, request } => {
                let groups_guard = self.groups.read().await;
                let layer = groups_guard.get(&group_id).ok_or_else(|| {
                    proven_network::NetworkError::ServiceError(format!(
                        "Group {group_id} not found"
                    ))
                })?;

                let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                let response = handler
                    .handle_vote(request)
                    .await
                    .map_err(|e| proven_network::NetworkError::ServiceError(e.to_string()))?;

                Ok(GroupConsensusServiceResponse::Vote { group_id, response })
            }
            GroupConsensusMessage::AppendEntries { group_id, request } => {
                let groups_guard = self.groups.read().await;
                let layer = groups_guard.get(&group_id).ok_or_else(|| {
                    proven_network::NetworkError::ServiceError(format!(
                        "Group {group_id} not found"
                    ))
                })?;

                let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                let response = handler
                    .handle_append_entries(request)
                    .await
                    .map_err(|e| proven_network::NetworkError::ServiceError(e.to_string()))?;

                Ok(GroupConsensusServiceResponse::AppendEntries { group_id, response })
            }
            GroupConsensusMessage::InstallSnapshot { group_id, request } => {
                let groups_guard = self.groups.read().await;
                let layer = groups_guard.get(&group_id).ok_or_else(|| {
                    proven_network::NetworkError::ServiceError(format!(
                        "Group {group_id} not found"
                    ))
                })?;

                let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                let response = handler
                    .handle_install_snapshot(request)
                    .await
                    .map_err(|e| proven_network::NetworkError::ServiceError(e.to_string()))?;

                Ok(GroupConsensusServiceResponse::InstallSnapshot { group_id, response })
            }
            GroupConsensusMessage::Consensus { group_id, request } => {
                let groups_guard = self.groups.read().await;
                let layer = groups_guard.get(&group_id).ok_or_else(|| {
                    proven_network::NetworkError::ServiceError(format!(
                        "Group {group_id} not found"
                    ))
                })?;

                let response = layer
                    .submit_request(request)
                    .await
                    .map_err(|e| proven_network::NetworkError::ServiceError(e.to_string()))?;

                Ok(GroupConsensusServiceResponse::Consensus { group_id, response })
            }
        }
    }
}
