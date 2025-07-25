//! Command handlers for membership service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::membership::{
    MembershipService,
    commands::{
        ClusterFormationResult, GetMembership, GetOnlineMembers, InitializeCluster, MembershipInfo,
    },
};
use proven_storage::StorageAdaptor;
use proven_topology::{TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

/// Handler for InitializeCluster command
#[derive(Clone)]
pub struct InitializeClusterHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, S>>,
}

impl<T, G, S> InitializeClusterHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<InitializeCluster> for InitializeClusterHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
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
pub struct GetMembershipHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, S>>,
}

impl<T, G, S> GetMembershipHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<GetMembership> for GetMembershipHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
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
pub struct GetOnlineMembersHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, S>>,
}

impl<T, G, S> GetOnlineMembersHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    pub fn new(service: Arc<MembershipService<T, G, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, S> RequestHandler<GetOnlineMembers> for GetOnlineMembersHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
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
