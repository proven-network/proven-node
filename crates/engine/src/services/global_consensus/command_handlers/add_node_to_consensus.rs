//! Handler for AddNodeToConsensus command

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use crate::consensus::global::GlobalConsensusLayer;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::AddNodeToConsensus;
use proven_storage::LogStorage;
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager};

/// Handler for AddNodeToConsensus command - adds nodes to the global Raft consensus
pub struct AddNodeToConsensusHandler<G, L>
where
    G: TopologyAdaptor,
    L: LogStorage + 'static,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
    topology_manager: Arc<TopologyManager<G>>,
}

impl<G, L> AddNodeToConsensusHandler<G, L>
where
    G: TopologyAdaptor,
    L: LogStorage + 'static,
{
    pub fn new(
        consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        Self {
            consensus_layer,
            topology_manager,
        }
    }
}

#[async_trait]
impl<G, L> RequestHandler<AddNodeToConsensus> for AddNodeToConsensusHandler<G, L>
where
    G: TopologyAdaptor + 'static,
    L: LogStorage + 'static,
{
    async fn handle(
        &self,
        request: AddNodeToConsensus,
        _metadata: EventMetadata,
    ) -> Result<Vec<NodeId>, EventError> {
        info!(
            "AddNodeToConsensusHandler: Adding node {} to global consensus",
            request.node_id
        );

        let node_id = request.node_id;

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Get node information from topology
        let node_info = self
            .topology_manager
            .get_node(&node_id)
            .await
            .ok_or_else(|| {
                EventError::Internal(format!("Could not find node info for {node_id}"))
            })?;

        // Let the consensus layer handle adding the node
        consensus
            .add_node(node_id, node_info)
            .await
            .map_err(|e| EventError::Internal(format!("Failed to add node to consensus: {e}")))?;

        info!("Successfully added node {} to global consensus", node_id);

        // Return the updated membership
        Ok(consensus.get_members())
    }
}
