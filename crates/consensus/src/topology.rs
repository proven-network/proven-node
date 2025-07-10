//! Network topology management and peer discovery

use ed25519_dalek::VerifyingKey;
use proven_governance::Governance;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::Node;
use crate::{
    NodeId,
    error::{ConsensusResult, Error},
};

/// Duration to wait before allowing another forced refresh for a missing peer
const MISSING_PEER_COOLDOWN: Duration = Duration::from_secs(30);

/// Manages network topology and peer discovery
pub struct TopologyManager<G>
where
    G: Governance + Send + Sync + 'static,
{
    cached_peers: Arc<RwLock<Vec<Node>>>,
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
        info!(
            "Refreshing topology from governance for node {}",
            self.node_id
        );

        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        info!("Governance returned {} nodes in topology", topology.len());

        let mut peers = Vec::new();

        for node in topology {
            info!(
                "  - Node {} at {}",
                NodeId::new(node.public_key),
                node.origin
            );

            // Skip ourselves
            if self.node_id == node.public_key {
                info!("    (skipping self)");
                continue;
            }

            // Validate the origin URL
            url::Url::parse(&node.origin).map_err(|e| {
                Error::Configuration(crate::error::ConfigurationError::InvalidValue {
                    key: "origin".to_string(),
                    reason: format!("Invalid origin URL '{}': {}", node.origin, e),
                })
            })?;

            peers.push(crate::Node::from(node));
        }

        info!("Found {} peers in topology (excluding self)", peers.len());

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
    pub async fn get_all_peers(&self) -> Vec<Node> {
        let cached_peers = self.cached_peers.read().await;
        cached_peers.clone()
    }

    /// Get a specific peer by public key
    /// Will attempt to refresh topology if peer is not found (unless in cooldown)
    pub async fn get_peer(&self, public_key: VerifyingKey) -> Option<Node> {
        let node_id = NodeId::new(public_key);

        // First check cached peers
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| peer.public_key() == public_key)
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
                .find(|peer| peer.public_key() == public_key)
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
    pub async fn get_peer_by_node_id(&self, node_id: &NodeId) -> Option<Node> {
        // First check cached peers
        {
            let cached_peers = self.cached_peers.read().await;
            if let Some(peer) = cached_peers
                .iter()
                .find(|peer| NodeId::new(peer.public_key()) == *node_id)
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
                .find(|peer| NodeId::new(peer.public_key()) == *node_id)
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
    pub async fn get_own_node(&self) -> ConsensusResult<Node> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        for node in topology {
            if self.node_id == node.public_key {
                return Ok(crate::Node::from(node));
            }
        }

        Err(Error::Configuration(
            crate::error::ConfigurationError::MissingRequired {
                key: format!(
                    "node with public key {}",
                    hex::encode(self.node_id.to_bytes())
                ),
            },
        ))
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

    /// Get all regions in the topology
    pub async fn get_regions(&self) -> ConsensusResult<Vec<String>> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let mut regions = HashSet::new();
        for node in topology {
            regions.insert(node.region);
        }

        Ok(regions.into_iter().collect())
    }

    /// Get all availability zones in a specific region
    pub async fn get_azs_in_region(&self, region: &str) -> ConsensusResult<Vec<String>> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let mut azs = HashSet::new();
        for node in topology {
            if node.region == region {
                azs.insert(node.availability_zone);
            }
        }

        Ok(azs.into_iter().collect())
    }

    /// Get all nodes in a specific region
    pub async fn get_nodes_by_region(&self, region: &str) -> ConsensusResult<Vec<crate::Node>> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region)
            .map(crate::Node::from)
            .collect();

        Ok(nodes)
    }

    /// Get all nodes in a specific region and availability zone
    pub async fn get_nodes_by_region_and_az(
        &self,
        region: &str,
        az: &str,
    ) -> ConsensusResult<Vec<crate::Node>> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region && node.availability_zone == az)
            .map(crate::Node::from)
            .collect();

        Ok(nodes)
    }

    /// Get the count of nodes in a specific region
    pub async fn get_node_count_by_region(&self, region: &str) -> ConsensusResult<usize> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let count = topology.iter().filter(|node| node.region == region).count();

        Ok(count)
    }

    /// Get the count of nodes in a specific region and optionally specific AZ
    pub async fn get_node_count_by_region_az(
        &self,
        region: &str,
        az: Option<&str>,
    ) -> ConsensusResult<usize> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        let count = topology
            .iter()
            .filter(|node| {
                let region_matches = node.region == region;
                match az {
                    Some(az) => region_matches && node.availability_zone == az,
                    None => region_matches,
                }
            })
            .count();

        Ok(count)
    }

    /// Get governance node details by NodeId
    pub async fn get_governance_node(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Option<proven_governance::GovernanceNode>> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(format!("Failed to get topology: {e}")))?;

        for node in topology {
            if *node_id == node.public_key {
                return Ok(Some(node));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
impl TopologyManager<proven_governance_mock::MockGovernance> {
    /// Add a node to the topology.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same public key already exists.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    pub fn add_node(&self, node: Node) -> Result<(), Error> {
        self.governance
            .add_node(node.into())
            .map_err(|e| Error::Governance(e.to_string()))
    }

    /// Remove a node from the topology by public key.
    ///
    /// # Errors
    ///
    /// Returns an error if no node with the given public key is found.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    pub fn remove_node(&self, node_id: NodeId) -> Result<(), Error> {
        self.governance
            .remove_node(*node_id.verifying_key())
            .map_err(|e| Error::Governance(e.to_string()))
    }

    /// Check if a node exists by public key.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    #[must_use]
    pub fn has_node(&self, node_id: NodeId) -> bool {
        self.governance.has_node(*node_id.verifying_key())
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
