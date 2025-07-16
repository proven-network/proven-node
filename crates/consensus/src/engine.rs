//! Main consensus engine
//!
//! This is a thin orchestration layer that coordinates services and consensus layers.
//! All business logic lives in the services or consensus modules.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::time::Duration;
use tracing::{error, info, warn};

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::NodeId;
use proven_transport::Transport;

use crate::error::{ConsensusError, ConsensusResult, ErrorKind};
use crate::foundation::{
    traits::ServiceCoordinator as ServiceCoordinatorTrait, types::ConsensusGroupId,
};
use crate::services::global_consensus::GlobalConsensusService;
use crate::services::group_consensus::GroupConsensusService;
use crate::services::{
    client::ClientService,
    cluster::{ClusterFormationEvent, ClusterService, FormationMode},
    event::EventService,
    lifecycle::LifecycleService,
    migration::MigrationService,
    monitoring::MonitoringService,
    network::NetworkService,
    pubsub::PubSubService,
    routing::RoutingService,
};
use crate::stream::StreamConfig;

use super::config::EngineConfig;
use super::coordinator::ServiceCoordinator;

/// Consensus engine
pub struct Engine<T, G, L>
where
    T: Transport,
    G: Governance,
    L: LogStorage,
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
    client_service: Arc<ClientService<T, G, L>>,

    /// Consensus services
    global_consensus_service: Option<Arc<GlobalConsensusService<T, G, L>>>,
    group_consensus_service: Option<Arc<GroupConsensusService<T, G, L>>>,

    /// Dependencies
    network_manager: Arc<NetworkManager<T, G>>,
    storage: L,

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

impl<T, G, L> Engine<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
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
        client_service: Arc<ClientService<T, G, L>>,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
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
            global_consensus_service: None,
            group_consensus_service: None,
            network_manager,
            storage,
            state: Arc::new(tokio::sync::RwLock::new(EngineState::NotInitialized)),
            topology_manager: None,
        }
    }

    /// Set the consensus services
    pub(super) fn set_consensus_services(
        &mut self,
        global: Arc<GlobalConsensusService<T, G, L>>,
        group: Arc<GroupConsensusService<T, G, L>>,
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

    /// Setup network service with engine handle (must be called before start)
    pub(super) fn setup_network_service_handle(&self) -> ConsensusResult<()> {
        // This is a bit of a hack, but we need to give the network service
        // a handle to the engine for routing consensus messages
        // We can't do this during construction due to circular dependencies

        // For now, we'll skip this and register handlers differently
        // TODO: Refactor to avoid circular dependency
        Ok(())
    }

    /// Start the engine
    pub async fn start(&mut self) -> ConsensusResult<()> {
        // Check state
        {
            let mut state = self.state.write().await;
            if *state != EngineState::NotInitialized {
                return Err(ConsensusError::with_context(
                    ErrorKind::InvalidState,
                    "Engine already started or stopped",
                ));
            }
            *state = EngineState::Initializing;
        }

        info!("Starting consensus engine for node {}", self.node_id);

        // 1. Start all services (including consensus services if configured)
        self.coordinator.start_all().await?;

        // 2. Run discovery and join/form cluster
        // The ClusterService will trigger the formation callback which the
        // GlobalConsensusService registered during builder setup
        self.cluster_service.discover_and_join().await?;

        // 3. Update state
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

        // Update state
        {
            let mut state = self.state.write().await;
            *state = EngineState::Stopped;
        }

        info!("Consensus engine stopped");
        Ok(())
    }

    /// Create a new stream
    pub async fn create_stream(&self, name: String, config: StreamConfig) -> ConsensusResult<()> {
        self.ensure_running().await?;

        // Would route through global consensus

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
    pub fn client(&self) -> crate::client::Client<T, G, L> {
        crate::client::Client::new(self.client_service.clone(), self.node_id.clone())
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
pub struct EngineHandleWrapper<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
{
    engine: Arc<tokio::sync::Mutex<Engine<T, G, L>>>,
}

impl<T, G, L> EngineHandleWrapper<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
{
    /// Create a new engine handle wrapper
    pub fn new(engine: Arc<tokio::sync::Mutex<Engine<T, G, L>>>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl<T, G, L> crate::services::network::EngineHandle for EngineHandleWrapper<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
{
    fn handle_cluster_join(
        &self,
        request: crate::services::cluster::ConsensusGroupJoinRequest,
    ) -> ConsensusResult<crate::services::cluster::ConsensusGroupJoinResponse> {
        // TODO: Implement cluster join handling
        Err(ConsensusError::with_context(
            ErrorKind::InvalidState,
            "Cluster join not implemented",
        ))
    }

    fn get_cluster_state(
        &self,
    ) -> Arc<tokio::sync::RwLock<crate::services::cluster::ClusterStateInfo>> {
        // TODO: Return actual cluster state
        Arc::new(tokio::sync::RwLock::new(
            crate::services::cluster::ClusterStateInfo::new(),
        ))
    }
}
