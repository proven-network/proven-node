//! Network service implementation with NetworkManager integration

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::services::cluster::ClusterStateInfo;
use crate::services::event::{Event, EventPublisher};
use crate::{
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::monitoring::MonitoringServiceMessage,
};

use super::types::NetworkStats;

/// Configuration for network service
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetworkConfig {
    /// Default request timeout
    pub default_timeout: Duration,
    /// Enable message compression
    pub enable_compression: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            enable_compression: false,
        }
    }
}

/// Network service for managing consensus network operations
pub struct NetworkService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Service configuration
    config: NetworkConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Network statistics
    stats: Arc<RwLock<NetworkStats>>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
    /// Cluster state for handlers
    cluster_state: Arc<RwLock<ClusterStateInfo>>,
}

impl<T, G> NetworkService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new network service
    pub fn new(
        config: NetworkConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            stats: Arc::new(RwLock::new(NetworkStats::default())),
            event_publisher: None,
            cluster_state: Arc::new(RwLock::new(ClusterStateInfo::new())),
        }
    }

    /// Set event publisher
    pub fn set_event_publisher(&mut self, publisher: EventPublisher) {
        self.event_publisher = Some(publisher);
    }

    /// Set cluster state
    pub fn set_cluster_state(&mut self, state: Arc<RwLock<ClusterStateInfo>>) {
        self.cluster_state = state;
    }

    /// Initialize the service
    pub async fn initialize(&mut self) -> ConsensusResult<()> {
        Ok(())
    }

    /// Get network statistics
    pub async fn stats(&self) -> NetworkStats {
        self.stats.read().await.clone()
    }

    /// Get node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get network manager reference
    pub fn network_manager(&self) -> &Arc<NetworkManager<T, G>> {
        &self.network_manager
    }
}
