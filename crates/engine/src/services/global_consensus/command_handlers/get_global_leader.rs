//! Handler for GetGlobalLeader command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::consensus::global::GlobalConsensusLayer;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::GetGlobalLeader;
use proven_storage::LogStorage;
use proven_topology::NodeId;

/// Handler for GetGlobalLeader command
pub struct GetGlobalLeaderHandler<L>
where
    L: LogStorage + 'static,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> GetGlobalLeaderHandler<L>
where
    L: LogStorage + 'static,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<GetGlobalLeader> for GetGlobalLeaderHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        _request: GetGlobalLeader,
        _metadata: EventMetadata,
    ) -> Result<Option<NodeId>, EventError> {
        // Query consensus layer directly
        let consensus_guard = self.consensus_layer.read().await;
        if let Some(consensus) = consensus_guard.as_ref() {
            // Return the current leader
            Ok(consensus.get_leader())
        } else {
            // Consensus layer not initialized yet
            Ok(None)
        }
    }
}
