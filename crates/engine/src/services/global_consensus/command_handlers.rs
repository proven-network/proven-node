//! Command handlers for global consensus service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::debug;

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::global_consensus::{
    GlobalConsensusService,
    commands::{
        AddNodeToConsensus, GetGlobalConsensusMembers, InitializeGlobalConsensus,
        RemoveNodeFromConsensus, SubmitGlobalRequest, UpdateGlobalMembership,
    },
};
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for SubmitGlobalRequest command
#[derive(Clone)]
pub struct SubmitGlobalRequestHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> SubmitGlobalRequestHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<SubmitGlobalRequest> for SubmitGlobalRequestHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: SubmitGlobalRequest,
        _metadata: EventMetadata,
    ) -> Result<crate::consensus::global::GlobalResponse, Error> {
        debug!("SubmitGlobalRequestHandler: Processing global request");

        match self.service.submit_request(request.request).await {
            Ok(response) => Ok(response),
            Err(e) => {
                // Preserve not-leader errors
                if e.is_not_leader() {
                    Err(Error::Internal(format!("NotLeader: {e}")))
                } else {
                    Err(Error::Internal(format!(
                        "Failed to submit global request: {e}"
                    )))
                }
            }
        }
    }
}

/// Handler for InitializeGlobalConsensus command
#[derive(Clone)]
pub struct InitializeGlobalConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> InitializeGlobalConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<InitializeGlobalConsensus>
    for InitializeGlobalConsensusHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: InitializeGlobalConsensus,
        _metadata: EventMetadata,
    ) -> Result<(), Error> {
        debug!(
            "InitializeGlobalConsensusHandler: Initializing global consensus with {} members",
            request.members.len()
        );

        self.service
            .initialize_cluster(request.members)
            .await
            .map_err(|e| Error::Internal(format!("Failed to initialize global consensus: {e}")))?;

        Ok(())
    }
}

/// Handler for UpdateGlobalMembership command
#[derive(Clone)]
pub struct UpdateGlobalMembershipHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> UpdateGlobalMembershipHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<UpdateGlobalMembership> for UpdateGlobalMembershipHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: UpdateGlobalMembership,
        _metadata: EventMetadata,
    ) -> Result<(), Error> {
        debug!(
            "UpdateGlobalMembershipHandler: Updating global consensus membership - add: {:?}, remove: {:?}",
            request.add_members, request.remove_members
        );

        self.service
            .update_membership(request.add_members, request.remove_members)
            .await
            .map_err(|e| Error::Internal(format!("Failed to update membership: {e}")))
    }
}

/// Handler for AddNodeToConsensus command
#[derive(Clone)]
pub struct AddNodeToConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> AddNodeToConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<AddNodeToConsensus> for AddNodeToConsensusHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: AddNodeToConsensus,
        _metadata: EventMetadata,
    ) -> Result<Vec<proven_topology::NodeId>, Error> {
        debug!(
            "AddNodeToConsensusHandler: Adding node {} to global consensus",
            request.node_id
        );

        self.service
            .add_node_to_consensus(request.node_id)
            .await
            .map_err(|e| Error::Internal(format!("Failed to add node to consensus: {e}")))
    }
}

/// Handler for RemoveNodeFromConsensus command
#[derive(Clone)]
pub struct RemoveNodeFromConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> RemoveNodeFromConsensusHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<RemoveNodeFromConsensus> for RemoveNodeFromConsensusHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: RemoveNodeFromConsensus,
        _metadata: EventMetadata,
    ) -> Result<Vec<proven_topology::NodeId>, Error> {
        debug!(
            "RemoveNodeFromConsensusHandler: Removing node {} from global consensus",
            request.node_id
        );

        self.service
            .remove_node_from_consensus(request.node_id)
            .await
            .map_err(|e| Error::Internal(format!("Failed to remove node from consensus: {e}")))
    }
}

/// Handler for GetGlobalConsensusMembers command
#[derive(Clone)]
pub struct GetGlobalConsensusMembersHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> GetGlobalConsensusMembersHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<GlobalConsensusService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<GetGlobalConsensusMembers>
    for GetGlobalConsensusMembersHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetGlobalConsensusMembers,
        _metadata: EventMetadata,
    ) -> Result<Vec<proven_topology::NodeId>, Error> {
        debug!("GetGlobalConsensusMembersHandler: Getting current global consensus members");

        Ok(self.service.get_members().await)
    }
}
