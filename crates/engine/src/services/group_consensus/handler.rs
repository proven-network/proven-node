//! Network service handler for group consensus

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};

use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
use super::types::{ConsensusLayers, States};
use crate::consensus::group::raft::GroupRaftMessageHandler;
use crate::foundation::GroupStateRead;

/// Group consensus service handler
pub struct GroupConsensusHandler<S>
where
    S: proven_storage::StorageAdaptor + 'static,
{
    groups: ConsensusLayers<S>,
    group_states: States,
}

impl<S> GroupConsensusHandler<S>
where
    S: proven_storage::StorageAdaptor + 'static,
{
    /// Create a new handler
    pub fn new(groups: ConsensusLayers<S>, group_states: States) -> Self {
        Self {
            groups,
            group_states,
        }
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
            GroupConsensusMessage::GetStreamState {
                group_id,
                stream_name,
            } => {
                // Check if we have this group locally
                let groups_guard = self.groups.read().await;
                if !groups_guard.contains_key(&group_id) {
                    return Ok(GroupConsensusServiceResponse::Error(format!(
                        "Group {group_id} not found"
                    )));
                }
                drop(groups_guard);

                // Get the state from our states map
                let states_guard = self.group_states.read().await;
                if let Some(state) = states_guard.get(&group_id) {
                    let stream_state = state.get_stream(&stream_name).await;
                    Ok(GroupConsensusServiceResponse::StreamState(stream_state))
                } else {
                    Ok(GroupConsensusServiceResponse::Error(format!(
                        "State for group {group_id} not found"
                    )))
                }
            }
        }
    }
}
