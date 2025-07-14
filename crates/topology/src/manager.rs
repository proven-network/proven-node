//! Network topology management and peer discovery

use async_trait::async_trait;
use ed25519_dalek::VerifyingKey;
use proven_bootable::Bootable;
use proven_governance::{Governance, GovernanceNode};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::{Node, NodeId, TopologyError};

/// Duration to wait before allowing another forced refresh for a missing peer
const MISSING_PEER_COOLDOWN: Duration = Duration::from_secs(30);

/// Bootable state for background tasks
struct BootableState {
    refresh_task: Option<JoinHandle<()>>,
    shutdown_signal: Option<oneshot::Sender<()>>,
}

/// Manages network topology and peer discovery
pub struct TopologyManager<G>
where
    G: Governance,
{
    cached_nodes: Arc<RwLock<Vec<Node>>>,
    governance: Arc<G>,
    node_id: NodeId,
    /// Track when we last tried to refresh topology for missing peers
    missing_peer_cooldown: Arc<RwLock<HashMap<NodeId, Instant>>>,
    /// Bootable state for background tasks
    bootable_state: Arc<RwLock<BootableState>>,
}

impl<G> TopologyManager<G>
where
    G: Governance,
{
    /// Create a new topology manager
    pub async fn new(governance: Arc<G>, node_id: NodeId) -> Result<Self, TopologyError> {
        info!("Creating topology manager for node {}", node_id);

        let topology_manager = Self {
            governance,
            node_id,
            cached_nodes: Arc::new(RwLock::new(Vec::new())),
            missing_peer_cooldown: Arc::new(RwLock::new(HashMap::new())),
            bootable_state: Arc::new(RwLock::new(BootableState {
                refresh_task: None,
                shutdown_signal: None,
            })),
        };

        topology_manager.refresh_topology().await?;

        Ok(topology_manager)
    }

    /// Start the topology manager (refresh topology)
    pub async fn start(&self) -> Result<(), TopologyError> {
        info!("Starting topology manager for node {}", self.node_id);
        self.refresh_topology().await?;
        Ok(())
    }

    /// Shutdown the topology manager
    pub async fn shutdown(&self) -> Result<(), TopologyError> {
        info!("Shutting down topology manager for node {}", self.node_id);
        Ok(())
    }

    /// Refresh topology from provider
    pub async fn refresh_topology(&self) -> Result<(), TopologyError> {
        info!(
            "Refreshing topology from provider for node {}",
            self.node_id
        );

        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        info!("Provider returned {} nodes in topology", topology.len());

        let mut nodes = Vec::new();

        for node in topology {
            info!(
                "  - Node {} at {}",
                NodeId::new(node.public_key),
                node.origin
            );

            // Validate the origin URL (warn but don't fail - important for tests)
            if let Err(e) = url::Url::parse(&node.origin) {
                warn!(
                    "Invalid origin URL '{}' for node {}: {}",
                    node.origin,
                    NodeId::new(node.public_key),
                    e
                );
            }

            nodes.push(Node::new(node));
        }

        info!("Found {} nodes in topology", nodes.len());

        // Update cached nodes
        {
            let mut cached_nodes = self.cached_nodes.write().await;
            *cached_nodes = nodes;
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
        self.cached_nodes
            .read()
            .await
            .iter()
            .filter(|node| *node.node_id() != self.node_id)
            .cloned()
            .collect()
    }

    /// Get a specific peer by public key
    /// Will attempt to refresh topology if peer is not found (unless in cooldown)
    pub async fn get_peer(&self, public_key: VerifyingKey) -> Option<Node> {
        let node_id = NodeId::new(public_key);

        // First check cached peers
        {
            let cached_peers = self.cached_nodes.read().await;
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
            let cached_peers = self.cached_nodes.read().await;
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

    /// Get a node by NodeId (includes self)
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        self.get_node_by_id(node_id).await
    }

    /// Get a specific node by NodeId (includes self)
    /// Will attempt to refresh topology if node is not found (unless in cooldown)
    pub async fn get_node_by_id(&self, node_id: &NodeId) -> Option<Node> {
        // First check cached nodes
        {
            let cached_nodes = self.cached_nodes.read().await;
            if let Some(node) = cached_nodes
                .iter()
                .find(|node| NodeId::new(node.public_key()) == *node_id)
                .cloned()
            {
                return Some(node);
            }
        }

        // Node not found in cache, check if we can refresh
        if self.is_in_cooldown(node_id).await {
            debug!("Node {} is in cooldown, skipping topology refresh", node_id);
            return None;
        }

        // Try refreshing topology
        debug!("Node {} not found in cache, refreshing topology", node_id);
        if let Err(e) = self.refresh_topology().await {
            warn!(
                "Failed to refresh topology while looking for node {}: {}",
                node_id, e
            );
            return None;
        }

        // Check again after refresh
        {
            let cached_nodes = self.cached_nodes.read().await;
            if let Some(node) = cached_nodes
                .iter()
                .find(|node| NodeId::new(node.public_key()) == *node_id)
                .cloned()
            {
                return Some(node);
            }
        }

        // Still not found, add to cooldown
        self.add_to_cooldown(node_id).await;
        None
    }

    /// Get a specific peer by NodeId (excludes self)
    /// Will attempt to refresh topology if peer is not found (unless in cooldown)
    pub async fn get_peer_by_node_id(&self, node_id: &NodeId) -> Option<Node> {
        // Don't return self as a peer
        if *node_id == self.node_id {
            return None;
        }

        self.get_node_by_id(node_id).await
    }

    /// Get provider reference
    pub fn provider(&self) -> &Arc<G> {
        &self.governance
    }

    /// Get our own GovernanceNode from the topology
    pub async fn get_own_node(&self) -> Result<Node, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        for node in topology {
            if self.node_id == NodeId::new(node.public_key) {
                return Ok(Node::new(node));
            }
        }

        Err(TopologyError::Configuration(format!(
            "node with public key {} not found in topology",
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

    /// Get all regions in the topology
    pub async fn get_regions(&self) -> Result<Vec<String>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let mut regions = HashSet::new();
        for node in topology {
            regions.insert(node.region);
        }

        Ok(regions.into_iter().collect())
    }

    /// Get all availability zones in a specific region
    pub async fn get_azs_in_region(&self, region: &str) -> Result<Vec<String>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let mut azs = HashSet::new();
        for node in topology {
            if node.region == region {
                azs.insert(node.availability_zone);
            }
        }

        Ok(azs.into_iter().collect())
    }

    /// Get all nodes in a specific region
    pub async fn get_nodes_by_region(&self, region: &str) -> Result<Vec<Node>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region)
            .map(Node::new)
            .collect();

        Ok(nodes)
    }

    /// Get all nodes in a specific region and availability zone
    pub async fn get_nodes_by_region_and_az(
        &self,
        region: &str,
        az: &str,
    ) -> Result<Vec<Node>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region && node.availability_zone == az)
            .map(Node::new)
            .collect();

        Ok(nodes)
    }

    /// Get the count of nodes in a specific region
    pub async fn get_node_count_by_region(&self, region: &str) -> Result<usize, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let count = topology.iter().filter(|node| node.region == region).count();

        Ok(count)
    }

    /// Get the count of nodes in a specific region and optionally specific AZ
    pub async fn get_node_count_by_region_az(
        &self,
        region: &str,
        az: Option<&str>,
    ) -> Result<usize, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

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
    ) -> Result<Option<GovernanceNode>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        for node in topology {
            if *node_id == NodeId::new(node.public_key) {
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    /// Get nodes by region and AZ grouped
    pub async fn get_nodes_by_region_az_grouped(
        &self,
        region: &str,
    ) -> Result<HashMap<String, Vec<Node>>, TopologyError> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| TopologyError::Governance(e.to_string()))?;

        let mut az_nodes: HashMap<String, Vec<Node>> = HashMap::new();

        for node in topology {
            if node.region == region {
                az_nodes
                    .entry(node.availability_zone.clone())
                    .or_default()
                    .push(Node::new(node));
            }
        }

        Ok(az_nodes)
    }

    // ============ Topology Analysis Methods ============

    /// Calculate AZ balance score for a region
    /// Returns a score from 0.0 (perfectly balanced) to 1.0 (completely imbalanced)
    pub fn calculate_az_balance_score(az_distribution: &HashMap<String, usize>) -> f64 {
        if az_distribution.is_empty() || az_distribution.len() == 1 {
            return 0.0;
        }

        let total_nodes: usize = az_distribution.values().sum();
        if total_nodes == 0 {
            return 0.0;
        }

        let mean = total_nodes as f64 / az_distribution.len() as f64;
        let variance: f64 = az_distribution
            .values()
            .map(|&count| {
                let diff = count as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / az_distribution.len() as f64;

        let std_dev = variance.sqrt();
        // Normalize to 0.0-1.0 range
        (std_dev / mean).min(1.0)
    }

    /// Calculate AZ diversity score for a group
    /// Returns a score from 0.0 (all in one AZ) to 1.0 (perfectly distributed)
    pub fn calculate_az_diversity_score(
        az_distribution: &HashMap<String, usize>,
        total_nodes: usize,
    ) -> f64 {
        if total_nodes == 0 || az_distribution.is_empty() {
            return 0.0;
        }

        // Calculate entropy
        let mut entropy = 0.0;
        for &count in az_distribution.values() {
            if count > 0 {
                let p = count as f64 / total_nodes as f64;
                entropy -= p * p.log2();
            }
        }

        // Normalize by maximum possible entropy
        let max_entropy = (az_distribution.len() as f64).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy
        } else {
            0.0
        }
    }
}

impl<G> Debug for TopologyManager<G>
where
    G: Governance + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologyManager")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

// Configuration for topology refresh
const TOPOLOGY_REFRESH_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

// Make TopologyManager cloneable
impl<G> Clone for TopologyManager<G>
where
    G: Governance,
{
    fn clone(&self) -> Self {
        Self {
            cached_nodes: Arc::clone(&self.cached_nodes),
            governance: Arc::clone(&self.governance),
            node_id: self.node_id.clone(),
            missing_peer_cooldown: Arc::clone(&self.missing_peer_cooldown),
            bootable_state: Arc::clone(&self.bootable_state),
        }
    }
}

#[async_trait]
impl<G> Bootable for TopologyManager<G>
where
    G: Governance,
{
    fn bootable_name(&self) -> &str {
        "TopologyManager"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting TopologyManager as bootable service");

        // Initial topology refresh
        self.refresh_topology().await?;

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Clone self for the background task
        let manager = Arc::new(self.clone());

        // Start background refresh task
        let refresh_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TOPOLOGY_REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        debug!("Performing periodic topology refresh");
                        if let Err(e) = manager.refresh_topology().await {
                            error!("Failed to refresh topology: {}", e);
                            // Continue running even if refresh fails
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("Topology refresh task received shutdown signal");
                        break;
                    }
                }
            }

            debug!("Topology refresh task exiting");
        });

        // Store the task handle and shutdown signal
        let mut state = self.bootable_state.write().await;
        state.refresh_task = Some(refresh_task);
        state.shutdown_signal = Some(shutdown_tx);

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down TopologyManager");

        let mut state = self.bootable_state.write().await;

        // Send shutdown signal
        if let Some(shutdown_tx) = state.shutdown_signal.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for the refresh task to complete
        if let Some(task) = state.refresh_task.take() {
            match tokio::time::timeout(Duration::from_secs(5), task).await {
                Ok(Ok(())) => {
                    debug!("Topology refresh task shut down cleanly");
                }
                Ok(Err(e)) => {
                    error!("Topology refresh task panicked: {}", e);
                }
                Err(_) => {
                    error!("Topology refresh task did not shut down within timeout");
                }
            }
        }

        // Clear cached nodes
        self.cached_nodes.write().await.clear();

        info!("TopologyManager shutdown complete");
        Ok(())
    }

    async fn wait(&self) {
        // Wait for the refresh task to complete (which should only happen on error)
        let task_handle = {
            let state = self.bootable_state.read().await;
            state.refresh_task.as_ref().map(|t| t.abort_handle())
        };

        if let Some(handle) = task_handle {
            // Wait for the task to finish
            loop {
                if handle.is_finished() {
                    error!("Topology refresh task failed unexpectedly");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use proven_governance::GovernanceNode;
    use proven_governance_mock::MockGovernance;

    #[cfg(test)]
    impl TopologyManager<MockGovernance> {
        /// Add a node to the topology.
        ///
        /// # Errors
        ///
        /// Returns an error if a node with the same public key already exists.
        ///
        /// # Panics
        ///
        /// Panics if the nodes cannot be locked.
        pub fn add_node(&self, node: Node) -> Result<(), TopologyError> {
            let governance_node = GovernanceNode {
                public_key: node.public_key(),
                origin: node.origin().to_string(),
                region: "us-east-1".to_string(), // Default for tests
                availability_zone: "us-east-1a".to_string(), // Default for tests
                specializations: Default::default(),
            };
            self.governance
                .add_node(governance_node)
                .map_err(|e| TopologyError::Governance(e.to_string()))
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
        pub fn remove_node(&self, node_id: NodeId) -> Result<(), TopologyError> {
            self.governance
                .remove_node(*node_id.verifying_key())
                .map_err(|e| TopologyError::Governance(e.to_string()))
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

    #[tokio::test]
    async fn test_topology_manager_creation() {
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let node_id = NodeId::from_seed(1);

        let topology_manager = TopologyManager::new(governance, node_id.clone())
            .await
            .unwrap();
        assert_eq!(topology_manager.node_id, node_id);
    }

    #[tokio::test]
    async fn test_get_all_peers_excludes_self() {
        let governance = Arc::new(MockGovernance::new(
            vec![],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));
        let node_id = NodeId::from_seed(1);

        // Create governance nodes
        let self_governance_node = GovernanceNode {
            public_key: *node_id.verifying_key(),
            origin: "http://localhost:8080".to_string(),
            region: "us-east-1".to_string(),
            availability_zone: "us-east-1a".to_string(),
            specializations: HashSet::new(),
        };

        let other_node_id = NodeId::from_seed(2);
        let other_governance_node = GovernanceNode {
            public_key: *other_node_id.verifying_key(),
            origin: "http://localhost:8081".to_string(),
            region: "us-east-1".to_string(),
            availability_zone: "us-east-1a".to_string(),
            specializations: HashSet::new(),
        };

        governance.add_node(self_governance_node).unwrap();
        governance.add_node(other_governance_node).unwrap();

        let topology_manager = TopologyManager::new(governance, node_id).await.unwrap();
        let peers = topology_manager.get_all_peers().await;

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].public_key(), *other_node_id.verifying_key());
    }
}
