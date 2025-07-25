//! Command handlers for global consensus service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::debug;

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::global_consensus::{
    GlobalConsensusService,
    commands::{InitializeGlobalConsensus, SubmitGlobalRequest},
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
