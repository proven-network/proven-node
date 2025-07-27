//! Command handlers for membership service

use async_trait::async_trait;
use proven_attestation::Attestor;
use std::sync::Arc;
use tracing::debug;

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::membership::{
    MembershipService,
    commands::{
        ClusterFormationResult, GetMembership, GetNodeInfo, GetOnlineMembers, GetPeerInfo,
        InitializeCluster, MembershipInfo,
    },
};
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for InitializeCluster command
#[derive(Clone)]
pub struct InitializeClusterHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> InitializeClusterHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<InitializeCluster> for InitializeClusterHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: InitializeCluster,
        _metadata: EventMetadata,
    ) -> Result<ClusterFormationResult, Error> {
        debug!("InitializeClusterHandler: Processing cluster initialization request");

        // TODO: Implement cluster initialization logic
        // For now, return a placeholder result
        let cluster_id = uuid::Uuid::new_v4().to_string();
        let members = vec![self.service.node_id().clone()];
        let coordinator = self.service.node_id().clone();

        Ok(ClusterFormationResult {
            cluster_id,
            members,
            coordinator,
        })
    }
}

/// Handler for GetMembership command
#[derive(Clone)]
pub struct GetMembershipHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> GetMembershipHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetMembership> for GetMembershipHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetMembership,
        _metadata: EventMetadata,
    ) -> Result<MembershipInfo, Error> {
        debug!("GetMembershipHandler: Processing membership info request");

        // Get membership view and construct info
        let view = self.service.get_membership_view().await;
        let members = view
            .get_members()
            .into_iter()
            .map(|(id, membership)| (id, membership.node_info.clone()))
            .collect();

        Ok(MembershipInfo {
            cluster_id: view.cluster_id().to_string(),
            members,
            coordinator: view.get_coordinator(),
            this_node: self.service.node_id().clone(),
        })
    }
}

/// Handler for GetOnlineMembers command
#[derive(Clone)]
pub struct GetOnlineMembersHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> GetOnlineMembersHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetOnlineMembers> for GetOnlineMembersHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetOnlineMembers,
        _metadata: EventMetadata,
    ) -> Result<Vec<(proven_topology::NodeId, proven_topology::Node)>, Error> {
        debug!("GetOnlineMembersHandler: Processing online members request");

        Ok(self.service.get_online_members().await)
    }
}

/// Handler for GetNodeInfo command
#[derive(Clone)]
pub struct GetNodeInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> GetNodeInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetNodeInfo> for GetNodeInfoHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        _request: GetNodeInfo,
        _metadata: EventMetadata,
    ) -> Result<proven_topology::Node, Error> {
        debug!("GetNodeInfoHandler: Getting current node info");

        // Return the node info stored in the service
        Ok(self.service.node_info().clone())
    }
}

/// Handler for GetPeerInfo command
#[derive(Clone)]
pub struct GetPeerInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> GetPeerInfoHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<GetPeerInfo> for GetPeerInfoHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetPeerInfo,
        _metadata: EventMetadata,
    ) -> Result<Option<proven_topology::Node>, Error> {
        debug!(
            "GetPeerInfoHandler: Getting info for peer {}",
            request.node_id
        );

        // Get peer info from topology manager
        Ok(self
            .service
            .topology_manager()
            .get_node(&request.node_id)
            .await)
    }
}
