//! Network service implementation with NetworkManager integration

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_transport::Transport;

use crate::services::cluster::ClusterStateInfo;
use crate::services::event::{Event, EventPublisher};
use crate::{
    error::{ConsensusError, ConsensusResult, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::monitoring::HealthCheckRequest,
};

use super::handlers::*;
use super::types::NetworkStats;

/// Handle for engine operations used by network handlers
#[async_trait::async_trait]
pub trait EngineHandle: Send + Sync {
    /// Handle cluster join request
    fn handle_cluster_join(
        &self,
        request: crate::services::cluster::ConsensusGroupJoinRequest,
    ) -> ConsensusResult<crate::services::cluster::ConsensusGroupJoinResponse>;
    /// Get cluster state
    fn get_cluster_state(&self) -> Arc<RwLock<ClusterStateInfo>>;
}

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
    G: Governance,
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
    /// Engine handle for handlers
    engine_handle: Option<Arc<dyn EngineHandle>>,
}

impl<T, G> NetworkService<T, G>
where
    T: Transport,
    G: Governance,
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
            engine_handle: None,
        }
    }

    /// Set event publisher
    pub fn set_event_publisher(&mut self, publisher: EventPublisher) {
        self.event_publisher = Some(publisher);
    }

    /// Set engine handle
    pub fn set_engine_handle(&mut self, handle: Arc<dyn EngineHandle>) {
        self.engine_handle = Some(handle);
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
