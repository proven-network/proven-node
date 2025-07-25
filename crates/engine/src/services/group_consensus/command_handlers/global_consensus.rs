//! Command handlers for global consensus commands

use std::sync::Arc;
use tracing::{debug, error, info};

use crate::consensus::group::types::{AdminOperation, GroupRequest, GroupResponse};
use crate::foundation::events::{EventMetadata, RequestHandler};
use crate::services::group_consensus::{
    GroupConsensusService,
    commands::{
        CreateGroup, DissolveGroup, GetGroupInfo, GetNodeGroups, GetStreamState, GroupInfo,
        InitializeStreamInGroup, SubmitToGroup,
    },
};
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for CreateGroup command from global consensus
#[derive(Clone)]
pub struct CreateGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> CreateGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<CreateGroup> for CreateGroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: CreateGroup,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        info!(
            "Creating local group {:?} with members: {:?}",
            request.group_id, request.members
        );

        if let Err(e) = self
            .group_consensus_service
            .create_group(request.group_id, request.members.clone())
            .await
        {
            error!("Failed to create local group {:?}: {}", request.group_id, e);
        } else {
            info!("Successfully created local group {:?}", request.group_id);
        }

        Ok(())
    }
}

/// Handler for DissolveGroup command
#[derive(Clone)]
pub struct DissolveGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> DissolveGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<DissolveGroup> for DissolveGroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: DissolveGroup,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        info!(
            "Group {:?} dissolved - would remove local instance",
            request.group_id
        );

        // TODO: Add dissolve_group method to GroupConsensusService
        // For now, just log that we would dissolve it

        Ok(())
    }
}

/// Handler for InitializeStreamInGroup command
#[derive(Clone)]
pub struct InitializeStreamInGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> InitializeStreamInGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<InitializeStreamInGroup> for InitializeStreamInGroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: InitializeStreamInGroup,
        _metadata: EventMetadata,
    ) -> Result<(), crate::foundation::events::Error> {
        info!(
            "Initializing stream {} in local group {:?}",
            request.stream_name, request.group_id
        );

        // Initialize the stream in group consensus
        let group_request = GroupRequest::Admin(AdminOperation::InitializeStream {
            stream: request.stream_name.clone(),
        });

        match self
            .group_consensus_service
            .submit_to_group(request.group_id, group_request)
            .await
        {
            Ok(_) => {
                debug!(
                    "Successfully initialized stream {} in group {:?}",
                    request.stream_name, request.group_id
                );
            }
            Err(e) if e.is_not_leader() => {
                // Not the leader - this is expected and fine
                debug!(
                    "Not the leader for group {:?}, stream {} will be initialized by leader",
                    request.group_id, request.stream_name
                );
            }
            Err(e) => {
                error!(
                    "Failed to initialize stream {} in group {:?}: {}",
                    request.stream_name, request.group_id, e
                );
            }
        }

        Ok(())
    }
}

/// Handler for SubmitToGroup command
#[derive(Clone)]
pub struct SubmitToGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> SubmitToGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<SubmitToGroup> for SubmitToGroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: SubmitToGroup,
        _metadata: EventMetadata,
    ) -> Result<GroupResponse, crate::foundation::events::Error> {
        match self
            .group_consensus_service
            .submit_to_group(request.group_id, request.request)
            .await
        {
            Ok(response) => Ok(response),
            Err(e) => Err(crate::foundation::events::Error::Internal(e.to_string())),
        }
    }
}

/// Handler for GetNodeGroups command
#[derive(Clone)]
pub struct GetNodeGroupsHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> GetNodeGroupsHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<GetNodeGroups> for GetNodeGroupsHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetNodeGroups,
        _metadata: EventMetadata,
    ) -> Result<Vec<crate::foundation::types::ConsensusGroupId>, crate::foundation::events::Error>
    {
        match self.group_consensus_service.get_node_groups().await {
            Ok(groups) => Ok(groups),
            Err(e) => {
                error!("Failed to get node groups: {}", e);
                Err(crate::foundation::events::Error::Internal(e.to_string()))
            }
        }
    }
}

/// Handler for GetGroupInfo command
#[derive(Clone)]
pub struct GetGroupInfoHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> GetGroupInfoHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<GetGroupInfo> for GetGroupInfoHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetGroupInfo,
        _metadata: EventMetadata,
    ) -> Result<Option<GroupInfo>, crate::foundation::events::Error> {
        match self
            .group_consensus_service
            .get_group_state_info(request.group_id)
            .await
        {
            Ok(state) => {
                // TODO: Get actual group members and leader from state
                Ok(Some(GroupInfo {
                    group_id: request.group_id,
                    members: vec![], // TODO: Extract from state
                    leader: None,    // TODO: Extract from state
                    streams: state
                        .streams
                        .into_iter()
                        .map(|s| crate::services::stream::StreamName::new(&s.name))
                        .collect(),
                }))
            }
            Err(e) => {
                error!("Failed to get group info for {:?}: {}", request.group_id, e);
                Err(crate::foundation::events::Error::Internal(e.to_string()))
            }
        }
    }
}

/// Handler for GetStreamState command
#[derive(Clone)]
pub struct GetStreamStateHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
}

impl<T, G, S> GetStreamStateHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(group_consensus_service: Arc<GroupConsensusService<T, G, S>>) -> Self {
        Self {
            group_consensus_service,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, S> RequestHandler<GetStreamState> for GetStreamStateHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetStreamState,
        _metadata: EventMetadata,
    ) -> Result<
        Option<crate::foundation::models::stream::StreamState>,
        crate::foundation::events::Error,
    > {
        match self
            .group_consensus_service
            .get_stream_state(request.group_id, request.stream_name.as_str())
            .await
        {
            Ok(state) => Ok(state),
            Err(e) => {
                error!(
                    "Failed to get stream state for {} in group {:?}: {}",
                    request.stream_name, request.group_id, e
                );
                Err(crate::foundation::events::Error::Internal(e.to_string()))
            }
        }
    }
}
