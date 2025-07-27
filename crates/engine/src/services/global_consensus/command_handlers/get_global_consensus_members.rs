//! Handler for GetGlobalConsensusMembers command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::consensus::global::GlobalConsensusLayer;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::GetGlobalConsensusMembers;
use proven_storage::LogStorage;
use proven_topology::NodeId;

/// Handler for GetGlobalConsensusMembers command
pub struct GetGlobalConsensusMembersHandler<L>
where
    L: LogStorage + 'static,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
}

impl<L> GetGlobalConsensusMembersHandler<L>
where
    L: LogStorage + 'static,
{
    pub fn new(consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<L> RequestHandler<GetGlobalConsensusMembers> for GetGlobalConsensusMembersHandler<L>
where
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        _request: GetGlobalConsensusMembers,
        _metadata: EventMetadata,
    ) -> Result<Vec<NodeId>, EventError> {
        // Query consensus layer directly
        let consensus_guard = self.consensus_layer.read().await;
        if let Some(consensus) = consensus_guard.as_ref() {
            // Return all voter IDs
            Ok(consensus.get_members())
        } else {
            // Consensus layer not initialized yet
            Ok(vec![])
        }
    }
}
