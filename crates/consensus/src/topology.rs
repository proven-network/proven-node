//! Network topology management and peer discovery

use ed25519_dalek::VerifyingKey;
use proven_governance::{Governance, GovernanceNode};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::{
    error::{ConsensusError, ConsensusResult},
    types::NodeId,
};

/// Manages network topology and peer discovery
pub struct TopologyManager<G>
where
    G: Governance + Send + Sync + 'static,
{
    cached_peers: Arc<RwLock<Vec<GovernanceNode>>>,
    governance: Arc<G>,
    node_id: NodeId,
}

impl<G> TopologyManager<G>
where
    G: Governance + Send + Sync + 'static + Debug,
{
    /// Create a new topology manager
    pub fn new(governance: Arc<G>, node_id: NodeId) -> Self {
        Self {
            governance,
            node_id,
            cached_peers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start the topology manager (refresh topology)
    pub async fn start(&self) -> ConsensusResult<()> {
        info!("Starting topology manager for node {}", self.node_id);
        self.refresh_topology().await?;
        Ok(())
    }

    /// Shutdown the topology manager
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        info!("Shutting down topology manager for node {}", self.node_id);
        Ok(())
    }

    /// Refresh topology from governance
    pub async fn refresh_topology(&self) -> ConsensusResult<()> {
        debug!("Refreshing topology from governance");

        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| ConsensusError::Governance(format!("Failed to get topology: {e}")))?;

        let mut peers = Vec::new();

        for node in topology {
            // Skip ourselves
            if self.node_id == node.public_key {
                continue;
            }

            // Validate the origin URL
            url::Url::parse(&node.origin).map_err(|e| {
                ConsensusError::Configuration(format!(
                    "Invalid origin URL '{}': {}",
                    node.origin, e
                ))
            })?;

            peers.push(node);
        }

        debug!("Found {} peers in topology", peers.len());

        // Update cached peers
        {
            let mut cached_peers = self.cached_peers.write().await;
            *cached_peers = peers;
        }

        Ok(())
    }

    /// Get all peers from cached topology
    pub async fn get_all_peers(&self) -> Vec<GovernanceNode> {
        let cached_peers = self.cached_peers.read().await;
        cached_peers.clone()
    }

    /// Get a specific peer by public key
    pub async fn get_peer(&self, public_key: VerifyingKey) -> Option<GovernanceNode> {
        let cached_peers = self.cached_peers.read().await;
        cached_peers
            .iter()
            .find(|peer| peer.public_key == public_key)
            .cloned()
    }

    /// Get a specific peer by NodeId
    pub async fn get_peer_by_node_id(&self, node_id: &NodeId) -> Option<GovernanceNode> {
        let cached_peers = self.cached_peers.read().await;
        cached_peers
            .iter()
            .find(|peer| NodeId::new(peer.public_key) == *node_id)
            .cloned()
    }

    /// Get governance reference
    pub fn governance(&self) -> &Arc<G> {
        &self.governance
    }

    /// Get our own GovernanceNode from the topology
    pub async fn get_own_node(&self) -> ConsensusResult<GovernanceNode> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| ConsensusError::Governance(format!("Failed to get topology: {e}")))?;

        for node in topology {
            if self.node_id == node.public_key {
                return Ok(node);
            }
        }

        Err(ConsensusError::Configuration(format!(
            "Own node with public key {} not found in topology",
            hex::encode(self.node_id.to_bytes())
        )))
    }
}

impl<G> Debug for TopologyManager<G>
where
    G: Governance + Send + Sync + 'static + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologyManager")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}
