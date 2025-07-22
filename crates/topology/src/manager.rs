//! Network topology management and peer discovery

use crate::adaptor::TopologyAdaptor;
use async_trait::async_trait;
use ed25519_dalek::VerifyingKey;
use proven_bootable::Bootable;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::subscriber::{TopologyBroadcaster, TopologySubscription};
use crate::{Node, NodeId, TopologyError};

/// Duration to wait before allowing another forced refresh for a missing peer
const MISSING_PEER_COOLDOWN: Duration = Duration::from_secs(30);

/// Default refresh interval for topology updates
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// Bootable state for background tasks
struct BootableState {
    refresh_task: Option<JoinHandle<()>>,
    shutdown_signal: Option<oneshot::Sender<()>>,
}

/// Configuration for the topology manager
#[derive(Clone)]
pub struct TopologyManagerConfig {
    /// Interval at which to refresh the topology
    pub refresh_interval: Duration,
}

impl Default for TopologyManagerConfig {
    fn default() -> Self {
        Self {
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }
}

/// Manages network topology and peer discovery
pub struct TopologyManager<T>
where
    T: TopologyAdaptor,
{
    cached_nodes: Arc<RwLock<Vec<Node>>>,
    topology_adaptor: Arc<T>,
    node_id: NodeId,
    /// Track when we last tried to refresh topology for missing peers
    missing_peer_cooldown: Arc<RwLock<HashMap<NodeId, Instant>>>,
    /// Bootable state for background tasks
    bootable_state: Arc<RwLock<BootableState>>,
    /// Broadcaster for topology changes
    broadcaster: Arc<TopologyBroadcaster>,
    /// Configuration
    config: TopologyManagerConfig,
}

impl<T> TopologyManager<T>
where
    T: TopologyAdaptor,
{
    /// Create a new topology manager with default configuration
    pub fn new(topology_adaptor: Arc<T>, node_id: NodeId) -> Self {
        Self::with_config(topology_adaptor, node_id, TopologyManagerConfig::default())
    }

    /// Create a new topology manager with custom configuration
    pub fn with_config(
        topology_adaptor: Arc<T>,
        node_id: NodeId,
        config: TopologyManagerConfig,
    ) -> Self {
        info!(
            "Creating topology manager for node {} with refresh interval {:?}",
            node_id, config.refresh_interval
        );

        Self {
            topology_adaptor,
            node_id,
            cached_nodes: Arc::new(RwLock::new(Vec::new())),
            missing_peer_cooldown: Arc::new(RwLock::new(HashMap::new())),
            bootable_state: Arc::new(RwLock::new(BootableState {
                refresh_task: None,
                shutdown_signal: None,
            })),
            broadcaster: Arc::new(TopologyBroadcaster::new(Vec::new())),
            config,
        }
    }

    /// Start the topology manager (refresh topology)
    pub async fn start(&self) -> Result<(), TopologyError> {
        info!("Starting topology manager for node {}", self.node_id);

        // Do initial refresh
        self.refresh_topology().await?;

        // Start background refresh task
        self.start_refresh_task().await;

        Ok(())
    }

    /// Shutdown the topology manager
    pub async fn shutdown(&self) -> Result<(), TopologyError> {
        info!("Shutting down topology manager for node {}", self.node_id);

        let mut bootable_state = self.bootable_state.write().await;

        // Send shutdown signal
        if let Some(shutdown_signal) = bootable_state.shutdown_signal.take() {
            let _ = shutdown_signal.send(());
        }

        // Wait for task to complete
        if let Some(task) = bootable_state.refresh_task.take() {
            match tokio::time::timeout(Duration::from_secs(5), task).await {
                Ok(Ok(())) => debug!("Topology refresh task completed"),
                Ok(Err(e)) => warn!("Topology refresh task failed: {}", e),
                Err(_) => warn!("Topology refresh task timed out"),
            }
        }

        Ok(())
    }

    /// Refresh topology from provider
    pub async fn refresh_topology(&self) -> Result<(), TopologyError> {
        info!(
            "Refreshing topology from provider for node {}",
            self.node_id
        );

        let topology = self
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        info!("Provider returned {} nodes in topology", topology.len());

        let mut nodes = Vec::new();

        for node in topology {
            info!("  - Node {} at {}", node.node_id, node.origin);

            // Validate the origin URL (warn but don't fail - important for tests)
            if let Err(e) = url::Url::parse(&node.origin) {
                warn!(
                    "Invalid origin URL '{}' for node {}: {}",
                    node.origin, node.node_id, e
                );
            }

            nodes.push(node);
        }

        info!("Found {} nodes in topology", nodes.len());

        // Check if topology actually changed
        let changed = {
            let cached_nodes = self.cached_nodes.read().await;

            // Check if size differs
            if cached_nodes.len() != nodes.len() {
                true
            } else {
                // Check if any node differs
                !nodes.iter().all(|new_node| {
                    cached_nodes.iter().any(|cached_node| {
                        cached_node.node_id == new_node.node_id
                            && cached_node.origin == new_node.origin
                            && cached_node.region == new_node.region
                            && cached_node.availability_zone == new_node.availability_zone
                    })
                })
            }
        };

        // Update cached nodes
        {
            let mut cached_nodes = self.cached_nodes.write().await;
            *cached_nodes = nodes.clone();
        }

        // Only notify subscribers if topology actually changed
        if changed {
            info!("Topology has changed, notifying subscribers");
            self.broadcaster.update(nodes);
        } else {
            debug!("Topology unchanged, skipping notification");
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
                .find(|peer| peer.node_id == node_id)
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
                .find(|peer| peer.node_id == node_id)
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
                .find(|node| node.node_id == *node_id)
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
                .find(|node| node.node_id == *node_id)
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
    pub fn provider(&self) -> &Arc<T> {
        &self.topology_adaptor
    }

    /// Get cached nodes
    pub async fn get_cached_nodes(&self) -> Result<Vec<Node>, TopologyError> {
        Ok(self.cached_nodes.read().await.clone())
    }

    /// Subscribe to topology changes
    pub fn subscribe(&self) -> TopologySubscription {
        self.broadcaster.subscribe()
    }

    /// Force an immediate refresh of the topology
    /// Useful for tests and scenarios where you need immediate updates
    pub async fn force_refresh(&self) -> Result<(), TopologyError> {
        info!("Forcing topology refresh for node {}", self.node_id);
        self.refresh_topology().await
    }

    /// Start the background refresh task
    async fn start_refresh_task(&self) {
        let cached_nodes = self.cached_nodes.clone();
        let topology_adaptor = self.topology_adaptor.clone();
        let node_id = self.node_id.clone();
        let broadcaster = self.broadcaster.clone();

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let refresh_interval = self.config.refresh_interval;
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        debug!("Refreshing topology for node {}", node_id);

                        match topology_adaptor.get_topology().await {
                            Ok(topology) => {
                                let mut nodes = Vec::new();
                                for node in topology {
                                    if let Err(e) = url::Url::parse(&node.origin) {
                                        warn!(
                                            "Invalid origin URL '{}' for node {}: {}",
                                            node.origin, node.node_id, e
                                        );
                                    }
                                    nodes.push(node);
                                }

                                debug!("Refreshed topology: {} nodes", nodes.len());

                                // Check if topology actually changed
                                let changed = {
                                    let cached = cached_nodes.read().await;

                                    // Check if size differs
                                    if cached.len() != nodes.len() {
                                        true
                                    } else {
                                        // Check if any node differs
                                        !nodes.iter().all(|new_node| {
                                            cached.iter().any(|cached_node| {
                                                cached_node.node_id == new_node.node_id &&
                                                cached_node.origin == new_node.origin &&
                                                cached_node.region == new_node.region &&
                                                cached_node.availability_zone == new_node.availability_zone
                                            })
                                        })
                                    }
                                };

                                // Update cached nodes
                                {
                                    let mut cached = cached_nodes.write().await;
                                    *cached = nodes.clone();
                                }

                                // Only notify subscribers if topology actually changed
                                if changed {
                                    debug!("Background refresh: topology changed, notifying subscribers");
                                    broadcaster.update(nodes);
                                } else {
                                    debug!("Background refresh: topology unchanged");
                                }
                            }
                            Err(e) => {
                                error!("Failed to refresh topology: {}", e);
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        info!("Topology refresh task shutting down");
                        break;
                    }
                }
            }
        });

        let mut bootable_state = self.bootable_state.write().await;
        bootable_state.refresh_task = Some(task);
        bootable_state.shutdown_signal = Some(shutdown_tx);
    }

    /// Get our own Node from the topology
    pub async fn get_own_node(&self) -> Result<Node, TopologyError> {
        let topology = self
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        for node in topology {
            if self.node_id == node.node_id {
                return Ok(node);
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
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        let mut regions = HashSet::new();
        for node in topology {
            regions.insert(node.region);
        }

        Ok(regions.into_iter().collect())
    }

    /// Get all availability zones in a specific region
    pub async fn get_azs_in_region(&self, region: &str) -> Result<Vec<String>, TopologyError> {
        let topology = self
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

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
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region)
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
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        let nodes = topology
            .into_iter()
            .filter(|node| node.region == region && node.availability_zone == az)
            .collect();

        Ok(nodes)
    }

    /// Get the count of nodes in a specific region
    pub async fn get_node_count_by_region(&self, region: &str) -> Result<usize, TopologyError> {
        let topology = self
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

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
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

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

    /// Get nodes by region and AZ grouped
    pub async fn get_nodes_by_region_az_grouped(
        &self,
        region: &str,
    ) -> Result<HashMap<String, Vec<Node>>, TopologyError> {
        let topology = self
            .topology_adaptor
            .get_topology()
            .await
            .map_err(|e| TopologyError::TopologyAdaptor(e.to_string()))?;

        let mut az_nodes: HashMap<String, Vec<Node>> = HashMap::new();

        for node in topology {
            if node.region == region {
                az_nodes
                    .entry(node.availability_zone.clone())
                    .or_default()
                    .push(node);
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

impl<T> Debug for TopologyManager<T>
where
    T: TopologyAdaptor + Debug,
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
impl<T> Clone for TopologyManager<T>
where
    T: TopologyAdaptor,
{
    fn clone(&self) -> Self {
        Self {
            cached_nodes: Arc::clone(&self.cached_nodes),
            topology_adaptor: Arc::clone(&self.topology_adaptor),
            node_id: self.node_id.clone(),
            missing_peer_cooldown: Arc::clone(&self.missing_peer_cooldown),
            bootable_state: Arc::clone(&self.bootable_state),
            broadcaster: Arc::clone(&self.broadcaster),
            config: self.config.clone(),
        }
    }
}

#[async_trait]
impl<T> Bootable for TopologyManager<T>
where
    T: TopologyAdaptor,
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
