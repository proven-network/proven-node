//! Network topology management and peer discovery.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use proven_bootable::Bootable;
use proven_governance::{Governance, TopologyNode};

use crate::error::{ConsensusError, ConsensusResult};

/// Default function for `last_seen` field.
fn default_instant() -> Instant {
    Instant::now()
}

/// Information about a peer node in the consensus network.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    /// The node's public key.
    pub public_key: String,

    /// The node's network address.
    pub address: SocketAddr,

    /// The node's availability zone.
    pub availability_zone: String,

    /// The node's region.
    pub region: String,

    /// Whether the peer is currently reachable.
    pub is_healthy: bool,

    /// Last time we successfully communicated with this peer.
    #[serde(skip_serializing, skip_deserializing, default = "default_instant")]
    pub last_seen: Instant,

    /// Specializations of this node.
    pub specializations: HashSet<String>,
}

/// Manages the network topology and peer connectivity.
#[derive(Clone, Debug)]
pub struct TopologyManager<G> {
    governance: Arc<G>,
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    local_public_key: String,
    refresh_interval: Duration,
    /// Shutdown signal sender.
    shutdown_tx: Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<()>>>>,
    /// Background task handle for lifecycle management.
    task_handle: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl<G> TopologyManager<G>
where
    G: Governance + Send + Sync + 'static,
{
    /// Creates a new topology manager.
    pub fn new(governance: Arc<G>, _local_node_id: String, local_public_key: String) -> Self {
        Self {
            governance,
            peers: Arc::new(RwLock::new(HashMap::new())),
            local_public_key,
            refresh_interval: Duration::from_secs(30),
            shutdown_tx: Arc::new(std::sync::Mutex::new(None)),
            task_handle: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Starts the topology refresh loop.
    /// Starts the topology manager
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if topology initialization fails.
    ///
    /// # Panics
    ///
    /// Panics if the shutdown mutex is poisoned.
    pub fn start_internal(&self) -> ConsensusResult<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

        let self_clone = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(self_clone.refresh_interval);

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Topology manager shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = self_clone.refresh_topology().await {
                            warn!("Failed to refresh topology: {}", e);
                        }
                    }
                }
            }
        });

        *self.task_handle.lock().unwrap() = Some(handle);
        Ok(())
    }

    /// Refreshes the topology from the governance system.
    /// Refreshes the topology by fetching latest peer information
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if topology refresh fails.
    pub async fn refresh_topology(&self) -> ConsensusResult<()> {
        debug!("Refreshing network topology from governance");

        let topology_nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| ConsensusError::Governance(e.to_string()))?;

        let mut peers = self.peers.write().await;
        let mut new_peers = HashMap::new();

        for node in topology_nodes {
            // Skip our own node
            if node.public_key == self.local_public_key {
                continue;
            }

            // Convert the topology node to peer info
            match self.topology_node_to_peer_info(node).await {
                Ok(peer_info) => {
                    new_peers.insert(peer_info.public_key.clone(), peer_info);
                }
                Err(e) => {
                    warn!("Failed to convert topology node to peer info: {}", e);
                }
            }
        }

        // Preserve health status and last_seen for existing peers
        for (key, new_peer) in &mut new_peers {
            if let Some(existing_peer) = peers.get(key) {
                new_peer.is_healthy = existing_peer.is_healthy;
                new_peer.last_seen = existing_peer.last_seen;
            }
        }

        *peers = new_peers;
        info!("Topology refreshed: {} peers", peers.len());

        Ok(())
    }

    /// Converts a topology node to peer info.
    #[allow(clippy::unused_async)]
    async fn topology_node_to_peer_info(&self, node: TopologyNode) -> ConsensusResult<PeerInfo> {
        // Parse the origin as a socket address
        let address: SocketAddr = node
            .origin
            .parse()
            .map_err(|e| ConsensusError::InvalidMessage(format!("Invalid peer address: {e}")))?;

        Ok(PeerInfo {
            public_key: node.public_key,
            address,
            availability_zone: node.availability_zone,
            region: node.region,
            is_healthy: false, // Will be updated by health checks
            last_seen: Instant::now(),
            specializations: node
                .specializations
                .into_iter()
                .map(|s| format!("{s:?}"))
                .collect(),
        })
    }

    /// Gets all healthy peers.
    pub async fn get_healthy_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers
            .values()
            .filter(|peer| peer.is_healthy)
            .cloned()
            .collect()
    }

    /// Gets all peers (including unhealthy ones).
    pub async fn get_all_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    /// Marks a peer as healthy.
    /// Marks a peer as healthy in the topology
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if the peer is not found or update fails.
    pub async fn mark_peer_healthy(&self, public_key: &str) -> ConsensusResult<()> {
        #[allow(clippy::significant_drop_tightening)]
        {
            let mut peers = self.peers.write().await;
            if let Some(peer) = peers.get_mut(public_key) {
                peer.is_healthy = true;
                peer.last_seen = Instant::now();
                debug!("Marked peer {} as healthy", public_key);
            } else {
                return Err(ConsensusError::NodeNotFound(public_key.to_string()));
            }
        }
        Ok(())
    }

    /// Marks a peer as unhealthy.
    /// Marks a peer as unhealthy in the topology
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if the peer is not found or update fails.
    pub async fn mark_peer_unhealthy(&self, public_key: &str) -> ConsensusResult<()> {
        #[allow(clippy::significant_drop_tightening)]
        {
            let mut peers = self.peers.write().await;
            if let Some(peer) = peers.get_mut(public_key) {
                peer.is_healthy = false;
                warn!("Marked peer {} as unhealthy", public_key);
            } else {
                return Err(ConsensusError::NodeNotFound(public_key.to_string()));
            }
        }
        Ok(())
    }

    /// Gets the number of healthy nodes.
    pub async fn healthy_node_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers.values().filter(|peer| peer.is_healthy).count() + 1 // +1 for self
    }

    /// Calculates the minimum number of nodes required for consensus (majority).
    pub async fn consensus_threshold(&self) -> usize {
        let total_nodes = self.peers.read().await.len() + 1; // +1 for self
        (total_nodes / 2) + 1
    }

    /// Checks if we have enough healthy nodes for consensus.
    pub async fn has_consensus_quorum(&self) -> bool {
        let healthy_count = self.healthy_node_count().await;
        let threshold = self.consensus_threshold().await;
        healthy_count >= threshold
    }

    /// Gets a peer by public key.
    pub async fn get_peer(&self, public_key: &str) -> Option<PeerInfo> {
        let peers = self.peers.read().await;
        peers.get(public_key).cloned()
    }
}

#[async_trait]
impl<G> Bootable for TopologyManager<G>
where
    G: Governance + Send + Sync + 'static,
{
    fn bootable_name(&self) -> &'static str {
        "TopologyManager"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.start_internal().map_err(std::convert::Into::into)
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Signal shutdown
        let value = self.shutdown_tx.lock().unwrap().take();
        if let Some(shutdown_tx) = value {
            let _ = shutdown_tx.send(());
        }

        // Take the handle first to avoid holding mutex across await
        let handle = self.task_handle.lock().unwrap().take();

        // Wait for the background task to complete
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                warn!("Error waiting for topology manager task: {}", e);
            }
        }

        info!("Topology manager shutdown complete");
        Ok(())
    }

    async fn wait(&self) {
        // If there's a running task, wait for it
        let handle = self.task_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                warn!("Error waiting for topology manager task: {}", e);
            }
        }
    }
}
