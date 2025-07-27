//! Handler for InitializeGlobalConsensus command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::{GlobalConsensusLayer, raft::GlobalRaftMessageHandler};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::InitializeGlobalConsensus;
use proven_storage::LogStorage;

/// Handler for InitializeGlobalConsensus command
pub struct InitializeGlobalConsensusHandler<L>
where
    L: LogStorage,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> InitializeGlobalConsensusHandler<L>
where
    L: LogStorage,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<InitializeGlobalConsensus> for InitializeGlobalConsensusHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: InitializeGlobalConsensus,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "InitializeGlobalConsensusHandler: Initializing global consensus cluster with {} members",
            request.members.len()
        );

        // Check if we have a consensus layer
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Check if Raft is already initialized
        if consensus.is_initialized().await {
            info!("Global consensus is already initialized, skipping initialization");
            return Ok(());
        }

        // Initialize cluster using the Raft handler
        let handler: &dyn GlobalRaftMessageHandler = consensus.as_ref();
        handler
            .initialize_cluster(request.members)
            .await
            .map_err(|e| {
                EventError::Internal(format!("Failed to initialize global consensus: {e}"))
            })?;

        info!("Successfully initialized global consensus Raft cluster");
        Ok(())
    }
}
