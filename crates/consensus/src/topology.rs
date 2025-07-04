//! Network topology management and peer discovery

use ed25519_dalek::VerifyingKey;
use proven_governance::{Governance, GovernanceNode};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::{
    error::{ConsensusError, ConsensusResult},
    types::NodeId,
};

/// Duration to wait before allowing another forced refresh for a missing peer
const MISSING_PEER_COOLDOWN: Duration = Duration::from_secs(30);

/// Manages network topology and peer discovery
pub struct TopologyManager<G>
where
    G: Governance + Send + Sync + 'static,
{
    cached_peers: Arc<RwLock<Vec<GovernanceNode>>>,
    governance: Arc<G>,
    node_id: NodeId,
    /// Track when we last tried to refresh topology for missing peers
    missing_peer_cooldown: Arc<RwLock<HashMap<NodeId, Instant>>>,
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
            missing_peer_cooldown: Arc::new(RwLock::new(HashMap::new())),
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

        // Clean up expired cooldowns while we're refreshing
        self.cleanup_expired_cooldowns().await;

        Ok(())
    }

    /// Check if a node is in cooldown for forced refresh
    async fn is_in_cooldown(&self, node_id: &NodeId) -> bool {
        let cooldown_map = self.missing_peer_cooldown.read().await;
        if let Some(last_attempt) = cooldown_map.get(node_id) {
            last_attempt.elapsed() < MISSING_PEER_COOLDOWN
        } else {
            false
        }
    }

    /// Add a node to the cooldown list
    async fn add_to_cooldown(&self, node_id: &NodeId) {
        let mut cooldown_map = self.missing_peer_cooldown.write().await;
        cooldown_map.insert(node_id.clone(), Instant::now());
        warn!("Added node {} to missing peer cooldown", node_id);
    }

    /// Clean up expired cooldown entries
    async fn cleanup_expired_cooldowns(&self) {
        let mut cooldown_map = self.missing_peer_cooldown.write().await;
        cooldown_map.retain(|_, last_attempt| last_attempt.elapsed() < MISSING_PEER_COOLDOWN);
    }

    /// Get all peers from cached topology
    pub async fn get_all_peers(&self) -> Vec<GovernanceNode> {
        let cached_peers = self.cached_peers.read().await;
        cached_peers.clone()
    }

    /// Get a specific peer by public key
    /// Will attempt to refresh topology if peer is not found (unless in cooldown)
    pub async fn get_peer(&self, public_key: VerifyingKey) -> Option<GovernanceNode> {
        let node_id = NodeId::new(public_key);

        // First check cached peers
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| peer.public_key == public_key)
                .cloned()
            {
                return Some(peer);
            }
        }

        // Peer not found in cache, check if we can refresh
        if self.is_in_cooldown(&node_id).await {
            debug!("Node {} is in cooldown, skipping topology refresh", node_id);
            return None;
        }

        // Try refreshing topology
        debug!("Peer {} not found in cache, refreshing topology", node_id);
        if let Err(e) = self.refresh_topology().await {
            warn!(
                "Failed to refresh topology while looking for peer {}: {}",
                node_id, e
            );
            return None;
        }

        // Check again after refresh
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| peer.public_key == public_key)
                .cloned()
            {
                return Some(peer);
            }
        }

        // Still not found, add to cooldown
        self.add_to_cooldown(&node_id).await;
        None
    }

    /// Get a specific peer by NodeId
    /// Will attempt to refresh topology if peer is not found (unless in cooldown)
    pub async fn get_peer_by_node_id(&self, node_id: &NodeId) -> Option<GovernanceNode> {
        // First check cached peers
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| NodeId::new(peer.public_key) == *node_id)
                .cloned()
            {
                return Some(peer);
            }
        }

        // Peer not found in cache, check if we can refresh
        if self.is_in_cooldown(node_id).await {
            debug!("Node {} is in cooldown, skipping topology refresh", node_id);
            return None;
        }

        // Try refreshing topology
        debug!("Peer {} not found in cache, refreshing topology", node_id);
        if let Err(e) = self.refresh_topology().await {
            warn!(
                "Failed to refresh topology while looking for peer {}: {}",
                node_id, e
            );
            return None;
        }

        // Check again after refresh
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| NodeId::new(peer.public_key) == *node_id)
                .cloned()
            {
                return Some(peer);
            }
        }

        // Still not found, add to cooldown
        self.add_to_cooldown(node_id).await;
        None
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

    /// Manually clear cooldown for a specific node (useful for testing or manual intervention)
    pub async fn clear_cooldown(&self, node_id: &NodeId) {
        let mut cooldown_map = self.missing_peer_cooldown.write().await;
        if cooldown_map.remove(node_id).is_some() {
            info!("Cleared cooldown for node {}", node_id);
        }
    }

    /// Get current cooldown status for debugging
    pub async fn get_cooldown_status(&self) -> HashMap<NodeId, Duration> {
        let cooldown_map = self.missing_peer_cooldown.read().await;
        cooldown_map
            .iter()
            .map(|(node_id, last_attempt)| {
                let elapsed = last_attempt.elapsed();
                let remaining = MISSING_PEER_COOLDOWN.saturating_sub(elapsed);
                (node_id.clone(), remaining)
            })
            .collect()
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
