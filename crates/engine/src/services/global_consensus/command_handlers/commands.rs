//! Command handlers for global consensus service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::consensus::global::raft::GlobalRaftMessageHandler;
use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::global_consensus::{
    GlobalConsensusService,
    commands::{
        CreateGroup, CreateStream, GetGlobalState, GlobalStateSnapshot, InitializeGlobalConsensus,
    },
};
use proven_storage::StorageAdaptor;
use proven_topology::{TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

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
        debug!("InitializeGlobalConsensusHandler: Processing initialization request");

        // Get topology manager to convert node IDs to nodes
        let topology_manager = self
            .service
            .topology_manager()
            .ok_or_else(|| Error::Internal("Topology manager not available".to_string()))?;

        // Convert NodeIds to BTreeMap<NodeId, Node>
        let mut members = std::collections::BTreeMap::new();
        for node_id in request.members {
            let node = topology_manager.get_node(&node_id).await.ok_or_else(|| {
                Error::Internal(format!("Node {} not found in topology", node_id))
            })?;
            members.insert(node_id, node);
        }

        // Initialize the cluster
        match self.service.initialize_cluster(members).await {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::Internal(format!(
                "Failed to initialize global consensus: {}",
                e
            ))),
        }
    }
}

/// Handler for CreateGroup command
#[derive(Clone)]
pub struct CreateGroupHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> CreateGroupHandler<T, G, S>
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
impl<T, G, S> RequestHandler<CreateGroup> for CreateGroupHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(&self, request: CreateGroup, _metadata: EventMetadata) -> Result<(), Error> {
        debug!(
            "CreateGroupHandler: Processing create group request for group {:?}",
            request.group_id
        );

        use crate::consensus::global::GlobalRequest;
        use crate::foundation::GroupInfo;

        let global_request = GlobalRequest::CreateGroup {
            info: GroupInfo {
                id: request.group_id,
                members: request.members,
            },
        };

        match self.service.submit_request(global_request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Internal(format!("Failed to create group: {}", e))),
        }
    }
}

/// Handler for CreateStream command
#[derive(Clone)]
pub struct CreateStreamHandler<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    service: Arc<GlobalConsensusService<T, G, S>>,
}

impl<T, G, S> CreateStreamHandler<T, G, S>
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
impl<T, G, S> RequestHandler<CreateStream> for CreateStreamHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: CreateStream,
        _metadata: EventMetadata,
    ) -> Result<crate::foundation::types::ConsensusGroupId, Error> {
        debug!(
            "CreateStreamHandler: Processing create stream request for {}",
            request.stream_name
        );

        use crate::consensus::global::GlobalRequest;
        use crate::services::stream::StreamInfo;

        let global_request = GlobalRequest::CreateStream {
            info: StreamInfo {
                name: request.stream_name,
                config: request.config,
            },
            target_group: request.target_group,
        };

        match self.service.submit_request(global_request).await {
            Ok(response) => match response {
                crate::consensus::global::GlobalResponse::StreamCreated { group_id, .. } => {
                    Ok(group_id)
                }
                _ => Err(Error::Internal("Unexpected response type".to_string())),
            },
            Err(e) => Err(Error::Internal(format!("Failed to create stream: {}", e))),
        }
    }
}
