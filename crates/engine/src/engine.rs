//! Main consensus engine
//!
//! This is a thin orchestration layer that coordinates services and consensus layers.
//! All business logic lives in the services or consensus modules.

use std::sync::Arc;

use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use tracing::{error, info};

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::RoutingTable;
use crate::foundation::{events::EventBus, traits::ServiceCoordinator as ServiceCoordinatorTrait};
use crate::services::global_consensus::GlobalConsensusService;
use crate::services::group_consensus::GroupConsensusService;
use crate::services::stream::StreamService;
use crate::services::{
    lifecycle::LifecycleService, migration::MigrationService, monitoring::MonitoringService,
    pubsub::PubSubService,
};

use super::config::EngineConfig;
use super::coordinator::ServiceCoordinator;

/// Consensus engine
pub struct Engine<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
    A: Attestor,
{
    /// Node ID
    node_id: NodeId,

    /// Engine configuration
    config: EngineConfig,

    /// Service coordinator
    coordinator: Arc<ServiceCoordinator>,

    /// Services
    monitoring_service: Arc<MonitoringService>,
    migration_service: Arc<MigrationService>,
    lifecycle_service: Arc<LifecycleService>,
    pubsub_service: Arc<PubSubService<T, G, A>>,
    stream_service: Arc<StreamService<T, G, A, S>>,

    /// Consensus services
    global_consensus_service: Option<Arc<GlobalConsensusService<T, G, A, S>>>,
    group_consensus_service: Option<Arc<GroupConsensusService<T, G, A, S>>>,

    /// Dependencies
    network_manager: Arc<NetworkManager<T, G, A>>,
    storage_manager: Arc<StorageManager<S>>,

    /// Topology manager for node information
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,

    /// Engine state
    state: Arc<tokio::sync::RwLock<EngineState>>,

    /// Routing table
    routing_table: Arc<RoutingTable>,

    /// Event bus
    event_bus: Arc<EventBus>,
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

impl<T, G, A, S> Engine<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + 'static,
{
    /// Create a new engine (use EngineBuilder instead)
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        node_id: NodeId,
        config: EngineConfig,
        coordinator: Arc<ServiceCoordinator>,
        monitoring_service: Arc<MonitoringService>,
        migration_service: Arc<MigrationService>,
        lifecycle_service: Arc<LifecycleService>,
        pubsub_service: Arc<PubSubService<T, G, A>>,
        stream_service: Arc<StreamService<T, G, A, S>>,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage_manager: Arc<StorageManager<S>>,
        routing_table: Arc<RoutingTable>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            config,
            coordinator,
            monitoring_service,
            migration_service,
            lifecycle_service,
            pubsub_service,
            stream_service,
            global_consensus_service: None,
            group_consensus_service: None,
            network_manager,
            storage_manager,
            state: Arc::new(tokio::sync::RwLock::new(EngineState::NotInitialized)),
            topology_manager: None,
            routing_table,
            event_bus,
        }
    }

    /// Set the consensus services
    pub(super) fn set_consensus_services(
        &mut self,
        global: Arc<GlobalConsensusService<T, G, A, S>>,
        group: Arc<GroupConsensusService<T, G, A, S>>,
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
                    return Err(Error::with_context(
                        ErrorKind::InvalidState,
                        format!("Engine cannot be started from {:?} state", *state),
                    ));
                }
            }
        }

        info!("Starting consensus engine for node {}", self.node_id);

        // 1. Start all services (including consensus services if configured)
        self.coordinator.start_all().await?;

        // 2. Global consensus initialization is now event-driven via membership events
        // The service will automatically initialize when it receives ClusterFormed event
        if self.global_consensus_service.is_none() {
            return Err(Error::with_context(
                ErrorKind::Configuration,
                "Global consensus service not configured",
            ));
        }
        info!("Global consensus will initialize via membership events");

        // 3. Wait for this node to join global consensus
        info!("Waiting for node to join global consensus...");
        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);

        loop {
            // Check if we're in global consensus
            match self.client().global_consensus_members().await {
                Ok(members) => {
                    if members.contains(&self.node_id) {
                        info!(
                            "Node successfully joined global consensus with {} total members",
                            members.len()
                        );
                        break;
                    }
                }
                Err(e) => {
                    // Service might not be ready yet
                    if start_time.elapsed() > timeout {
                        return Err(Error::with_context(
                            ErrorKind::Timeout,
                            format!(
                                "Failed to join global consensus after {}s: {}",
                                timeout.as_secs(),
                                e
                            ),
                        ));
                    }
                }
            }

            if start_time.elapsed() > timeout {
                return Err(Error::with_context(
                    ErrorKind::Timeout,
                    format!(
                        "Timeout waiting to join global consensus after {}s",
                        timeout.as_secs()
                    ),
                ));
            }

            // Brief sleep before retry
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

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

    /// Get a client for interacting with the consensus engine
    pub fn client(&self) -> crate::client::Client {
        crate::client::Client::new(self.node_id.clone(), self.event_bus.clone())
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
