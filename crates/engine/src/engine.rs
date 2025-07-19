//! Main consensus engine
//!
//! This is a thin orchestration layer that coordinates services and consensus layers.
//! All business logic lives in the services or consensus modules.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::time::Duration;
use tracing::{error, info, warn};

use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use crate::error::{ConsensusError, ConsensusResult, ErrorKind};
use crate::foundation::{
    traits::ServiceCoordinator as ServiceCoordinatorTrait, types::ConsensusGroupId,
};
use crate::services::global_consensus::GlobalConsensusService;
use crate::services::group_consensus::GroupConsensusService;
use crate::services::stream::StreamService;
use crate::services::{
    client::ClientService,
    cluster::{ClusterFormationEvent, ClusterInfo, ClusterService, FormationMode},
    event::EventService,
    lifecycle::LifecycleService,
    migration::MigrationService,
    monitoring::MonitoringService,
    network::NetworkService,
    pubsub::PubSubService,
    routing::RoutingService,
};

use super::config::EngineConfig;
use super::coordinator::ServiceCoordinator;

/// Consensus engine
pub struct Engine<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,

    /// Engine configuration
    config: EngineConfig,

    /// Service coordinator
    coordinator: Arc<ServiceCoordinator>,

    /// Services
    network_service: Arc<NetworkService<T, G>>,
    cluster_service: Arc<ClusterService<T, G>>,
    event_service: Arc<EventService>,
    monitoring_service: Arc<MonitoringService>,
    routing_service: Arc<RoutingService>,
    migration_service: Arc<MigrationService>,
    lifecycle_service: Arc<LifecycleService>,
    pubsub_service: Arc<PubSubService<T, G>>,
    client_service: Arc<ClientService<T, G, S>>,
    stream_service: Arc<StreamService<S>>,

    /// Consensus services
    global_consensus_service: Option<Arc<GlobalConsensusService<T, G, S>>>,
    group_consensus_service: Option<Arc<GroupConsensusService<T, G, S>>>,

    /// Dependencies
    network_manager: Arc<NetworkManager<T, G>>,
    storage_manager: Arc<StorageManager<S>>,

    /// Topology manager for node information
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,

    /// Engine state
    state: Arc<tokio::sync::RwLock<EngineState>>,
}

/// Engine state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EngineState {
    /// Not initialized
    NotInitialized,
    /// Initializing
    Initializing,
    /// Running
    Running,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
}

impl<T, G, S> Engine<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new engine (use EngineBuilder instead)
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        node_id: NodeId,
        config: EngineConfig,
        coordinator: Arc<ServiceCoordinator>,
        network_service: Arc<NetworkService<T, G>>,
        cluster_service: Arc<ClusterService<T, G>>,
        event_service: Arc<EventService>,
        monitoring_service: Arc<MonitoringService>,
        routing_service: Arc<RoutingService>,
        migration_service: Arc<MigrationService>,
        lifecycle_service: Arc<LifecycleService>,
        pubsub_service: Arc<PubSubService<T, G>>,
        client_service: Arc<ClientService<T, G, S>>,
        stream_service: Arc<StreamService<S>>,
        network_manager: Arc<NetworkManager<T, G>>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            node_id,
            config,
            coordinator,
            network_service,
            cluster_service,
            event_service,
            monitoring_service,
            routing_service,
            migration_service,
            lifecycle_service,
            pubsub_service,
            client_service,
            stream_service,
            global_consensus_service: None,
            group_consensus_service: None,
            network_manager,
            storage_manager,
            state: Arc::new(tokio::sync::RwLock::new(EngineState::NotInitialized)),
            topology_manager: None,
        }
    }

    /// Set the consensus services
    pub(super) fn set_consensus_services(
        &mut self,
        global: Arc<GlobalConsensusService<T, G, S>>,
        group: Arc<GroupConsensusService<T, G, S>>,
    ) {
        self.global_consensus_service = Some(global);
        self.group_consensus_service = Some(group);
    }

    /// Set the topology manager
    pub(super) fn set_topology_manager(
        &mut self,
        topology: Arc<proven_topology::TopologyManager<G>>,
    ) {
        self.topology_manager = Some(topology);
    }

    /// Start the engine
    pub async fn start(&mut self) -> ConsensusResult<()> {
        // Check state
        {
            let mut state = self.state.write().await;
            match *state {
                EngineState::NotInitialized | EngineState::Stopped => {
                    *state = EngineState::Initializing;
                }
                _ => {
                    return Err(ConsensusError::with_context(
                        ErrorKind::InvalidState,
                        format!("Engine cannot be started from {:?} state", *state),
                    ));
                }
            }
        }

        info!("Starting consensus engine for node {}", self.node_id);

        // 1. Start all services (including consensus services if configured)
        self.coordinator.start_all().await?;

        // 2. Run discovery and join/form cluster
        // The ClusterService will trigger the formation callback which the
        // GlobalConsensusService registered during builder setup
        self.cluster_service.discover_and_join().await?;

        // 3. Wait for default group to be created
        // This ensures the engine is ready to handle stream operations
        self.wait_for_default_group(Duration::from_secs(30)).await?;

        // 4. Update state
        {
            let mut state = self.state.write().await;
            *state = EngineState::Running;
        }

        info!("Consensus engine started successfully");
        Ok(())
    }

    /// Stop the engine
    pub async fn stop(&mut self) -> ConsensusResult<()> {
        // Check state
        {
            let mut state = self.state.write().await;
            if *state != EngineState::Running {
                return Ok(());
            }
            *state = EngineState::Stopping;
        }

        info!("Stopping consensus engine");

        // Stop all services (coordinator handles reverse order)
        if let Err(e) = self.coordinator.stop_all().await {
            error!("Error stopping services: {}", e);
        }

        // Note: We intentionally do NOT shut down the storage manager here
        // This allows the engine to be restarted without releasing storage locks
        // The storage will be properly shut down when the storage manager is dropped
        info!("Engine services stopped, storage remains available for restart");

        // Update state
        {
            let mut state = self.state.write().await;
            *state = EngineState::Stopped;
        }

        info!("Consensus engine stopped");
        Ok(())
    }

    /// Ensure engine is running
    async fn ensure_running(&self) -> ConsensusResult<()> {
        let state = self.state.read().await;
        if *state != EngineState::Running {
            return Err(ConsensusError::with_context(
                ErrorKind::InvalidState,
                "Engine not running",
            ));
        }
        Ok(())
    }

    /// Wait for the default group to be created
    async fn wait_for_default_group(&self, timeout: Duration) -> ConsensusResult<()> {
        use tokio::time::timeout as tokio_timeout;

        let start = std::time::Instant::now();
        let default_group_id = crate::foundation::types::ConsensusGroupId::new(1);

        // Try to wait for the default group to exist in the routing table
        let result = tokio_timeout(timeout, async {
            loop {
                // Check if the routing service knows about the default group
                // Check if the default group exists in the routing table
                match self
                    .routing_service
                    .get_group_location(default_group_id)
                    .await
                {
                    Ok(location_info) => {
                        info!(
                            "Default group (ID 1) found in routing table with {} nodes",
                            location_info.nodes.len()
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        // Group might not exist in routing table yet
                        tracing::debug!("Default group not in routing table yet: {}", e);
                    }
                }

                // Check timeout
                if start.elapsed() > timeout {
                    break;
                }

                // Wait a bit before checking again
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            Err(ConsensusError::with_context(
                ErrorKind::Timeout,
                format!("Timeout waiting for default group after {timeout:?}"),
            ))
        })
        .await;

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(ConsensusError::with_context(
                ErrorKind::Timeout,
                format!("Timeout waiting for default group after {timeout:?}"),
            )),
        }
    }

    /// Get engine health
    pub async fn health(&self) -> ConsensusResult<EngineHealth> {
        let state = *self.state.read().await;
        let service_health = self.coordinator.all_healthy().await;

        Ok(EngineHealth {
            state,
            services_healthy: service_health,
            consensus_healthy: true, // Would check actual consensus health
        })
    }

    /// Get PubSub service
    pub fn pubsub_service(&self) -> Arc<PubSubService<T, G>> {
        self.pubsub_service.clone()
    }

    /// Get event service
    pub fn event_service(&self) -> Arc<EventService> {
        self.event_service.clone()
    }

    /// Get a client for interacting with the consensus engine
    pub fn client(&self) -> crate::client::Client<T, G, S> {
        crate::client::Client::new(self.client_service.clone(), self.node_id.clone())
    }

    /// Get current cluster state information
    pub async fn cluster_state(&self) -> ConsensusResult<ClusterInfo> {
        self.cluster_service.get_cluster_info().await.map_err(|e| {
            ConsensusError::with_context(
                ErrorKind::Internal,
                format!("Failed to get cluster state: {e}"),
            )
        })
    }

    /// Get all group IDs this node is a member of
    pub async fn node_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let group_consensus = self.group_consensus_service.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                ErrorKind::InvalidState,
                "Group consensus service not initialized",
            )
        })?;

        group_consensus.get_node_groups().await.map_err(|e| {
            ConsensusError::with_context(
                ErrorKind::Internal,
                format!("Failed to get node groups: {e}"),
            )
        })
    }

    /// Get group state information
    pub async fn group_state(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<crate::services::group_consensus::GroupStateInfo> {
        let group_consensus = self.group_consensus_service.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                ErrorKind::InvalidState,
                "Group consensus service not initialized",
            )
        })?;

        group_consensus
            .get_group_state_info(group_id)
            .await
            .map_err(|e| {
                ConsensusError::with_context(
                    ErrorKind::Internal,
                    format!("Failed to get group state: {e}"),
                )
            })
    }
}

/// Engine health information
#[derive(Debug, Clone)]
pub struct EngineHealth {
    /// Engine state
    pub state: EngineState,
    /// Services health
    pub services_healthy: bool,
    /// Consensus health
    pub consensus_healthy: bool,
}

/// Engine handle wrapper for network service
pub struct EngineHandleWrapper<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    engine: Arc<tokio::sync::Mutex<Engine<T, G, S>>>,
}

impl<T, G, S> EngineHandleWrapper<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new engine handle wrapper
    pub fn new(engine: Arc<tokio::sync::Mutex<Engine<T, G, S>>>) -> Self {
        Self { engine }
    }
}
