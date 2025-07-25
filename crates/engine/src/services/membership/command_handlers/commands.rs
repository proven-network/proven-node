//! Command handlers for membership service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::membership::{
    MembershipService,
    commands::{ClusterFormationResult, GetMembership, InitializeCluster, MembershipInfo},
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
        request: InitializeCluster,
        _metadata: EventMetadata,
    ) -> Result<ClusterFormationResult, Error> {
        debug!("InitializeClusterHandler: Processing cluster initialization request");

        match self.service.initialize_cluster(request.strategy).await {
            Ok(result) => Ok(result),
            Err(e) => Err(Error::Internal(format!(
                "Failed to initialize cluster: {}",
                e
            ))),
        }
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

        match self.service.get_membership_info().await {
            Ok(info) => Ok(info),
            Err(e) => Err(Error::Internal(format!(
                "Failed to get membership info: {}",
                e
            ))),
        }
    }
}
