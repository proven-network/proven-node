//! Engine builder pattern

use std::sync::Arc;

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;

use crate::error::{ConsensusError, ConsensusResult, ErrorKind};
use crate::foundation::traits::ServiceLifecycle;
use crate::services::{
    client::ClientService, cluster::ClusterService, event::EventService,
    lifecycle::LifecycleService, migration::MigrationService, monitoring::MonitoringService,
    network::NetworkService, pubsub::PubSubService, routing::RoutingService,
};
use tracing::{error, info};

use super::config::EngineConfig;
use super::coordinator::ServiceCoordinator;
use super::engine::Engine;

/// Engine builder
pub struct EngineBuilder<T, G, L>
where
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Node ID
    node_id: NodeId,

    /// Configuration
    config: Option<EngineConfig>,

    /// Network manager
    network_manager: Option<Arc<NetworkManager<T, G>>>,

    /// Topology manager
    topology_manager: Option<Arc<TopologyManager<G>>>,

    /// Storage
    storage: Option<L>,
}

impl<T, G, L> EngineBuilder<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
{
    /// Create a new engine builder
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            config: None,
            network_manager: None,
            topology_manager: None,
            storage: None,
        }
    }

    /// Set configuration
    pub fn with_config(mut self, config: EngineConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set network manager
    pub fn with_network(mut self, network: Arc<NetworkManager<T, G>>) -> Self {
        self.network_manager = Some(network);
        self
    }

    /// Set topology manager
    pub fn with_topology(mut self, topology: Arc<TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology);
        self
    }

    /// Set storage
    pub fn with_storage(mut self, storage: L) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Build the engine
    pub async fn build(self) -> ConsensusResult<Engine<T, G, L>> {
        // Validate required fields
        let config = self.config.ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::Configuration, "Config not set")
        })?;

        let network_manager = self.network_manager.ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::Configuration, "Network manager not set")
        })?;

        let topology_manager = self.topology_manager.as_ref().ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::Configuration, "Topology manager not set")
        })?;

        let storage = self.storage.ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::Configuration, "Storage not set")
        })?;

        // Create service coordinator
        let coordinator = Arc::new(ServiceCoordinator::new());

        // Create network service first as others depend on it
        let network_service = NetworkService::new(
            crate::services::network::NetworkConfig::default(),
            self.node_id.clone(),
            network_manager.clone(),
        );

        // Create other services
        let mut event_service = EventService::new(config.services.event.clone());
        event_service.start().await?;
        let event_service = Arc::new(event_service);

        // Create consensus services
        use crate::services::global_consensus::{GlobalConsensusConfig, GlobalConsensusService};
        use crate::services::group_consensus::{GroupConsensusConfig, GroupConsensusService};

        let global_consensus_config = GlobalConsensusConfig {
            election_timeout_min: config.consensus.global.election_timeout_min,
            election_timeout_max: config.consensus.global.election_timeout_max,
            heartbeat_interval: config.consensus.global.heartbeat_interval,
            max_entries_per_append: config.consensus.global.max_entries_per_append,
            snapshot_interval: config.consensus.global.snapshot_interval,
        };

        let global_consensus_service = Arc::new(
            GlobalConsensusService::new(
                global_consensus_config,
                self.node_id.clone(),
                network_manager.clone(),
                storage.clone(),
            )
            .with_topology(topology_manager.clone())
            .with_event_publisher(event_service.create_publisher()),
        );

        let group_consensus_config = GroupConsensusConfig {
            election_timeout_min: config.consensus.group.election_timeout_min,
            election_timeout_max: config.consensus.group.election_timeout_max,
            heartbeat_interval: config.consensus.group.heartbeat_interval,
            max_entries_per_append: config.consensus.group.max_entries_per_append,
            snapshot_interval: config.consensus.group.snapshot_interval,
        };

        let group_consensus_service = Arc::new(
            GroupConsensusService::new(
                group_consensus_config,
                self.node_id.clone(),
                network_manager.clone(),
                storage.clone(),
            )
            .with_event_publisher(event_service.create_publisher()),
        );

        // Get the formation callback from the global consensus service
        let formation_callback = global_consensus_service.get_formation_callback();

        let cluster_service = Arc::new(
            ClusterService::new(config.services.cluster.clone(), self.node_id.clone())
                .with_discovery(
                    network_manager.clone(),
                    topology_manager.clone(),
                    event_service.create_publisher(),
                )
                .with_formation_callback(formation_callback),
        );

        // Create system view for monitoring
        let system_view = Arc::new(crate::services::monitoring::SystemView::new());
        let monitoring_service = Arc::new(MonitoringService::new(
            config.services.monitoring.clone(),
            self.node_id.clone(),
            system_view,
        ));

        let routing_service = Arc::new(RoutingService::new(config.services.routing.clone()));

        let migration_service = Arc::new(MigrationService::new(config.services.migration.clone()));

        let lifecycle_service = Arc::new(LifecycleService::new(config.services.lifecycle.clone()));

        // Create PubSub service
        let pubsub_service = Arc::new(PubSubService::new(
            config.services.pubsub.clone(),
            self.node_id.clone(),
        ));

        // Create ClientService
        let client_service = Arc::new(ClientService::new(self.node_id.clone()));

        // Create service wrappers that implement ServiceLifecycle
        let network_service = Arc::new(network_service);
        let network_wrapper = Arc::new(ServiceWrapper::new("network", network_service.clone()));
        let cluster_wrapper = Arc::new(ServiceWrapper::new("cluster", cluster_service.clone()));
        let event_wrapper = Arc::new(ServiceWrapper::new("event", event_service.clone()));
        let monitoring_wrapper = Arc::new(ServiceWrapper::new(
            "monitoring",
            monitoring_service.clone(),
        ));
        let routing_wrapper = Arc::new(ServiceWrapper::new("routing", routing_service.clone()));
        let migration_wrapper =
            Arc::new(ServiceWrapper::new("migration", migration_service.clone()));
        let lifecycle_wrapper =
            Arc::new(ServiceWrapper::new("lifecycle", lifecycle_service.clone()));
        let pubsub_wrapper = Arc::new(ServiceWrapper::new("pubsub", pubsub_service.clone()));
        let client_wrapper = Arc::new(ServiceWrapper::new("client", client_service.clone()));

        // Create consensus service wrappers
        let global_consensus_wrapper = Arc::new(ServiceWrapper::new(
            "global_consensus",
            global_consensus_service.clone(),
        ));
        let group_consensus_wrapper = Arc::new(ServiceWrapper::new(
            "group_consensus",
            group_consensus_service.clone(),
        ));

        // Register services with coordinator
        coordinator
            .register("network".to_string(), network_wrapper)
            .await;
        coordinator
            .register("event".to_string(), event_wrapper)
            .await;
        coordinator
            .register("monitoring".to_string(), monitoring_wrapper)
            .await;
        coordinator
            .register("cluster".to_string(), cluster_wrapper)
            .await;
        coordinator
            .register("routing".to_string(), routing_wrapper)
            .await;
        coordinator
            .register("migration".to_string(), migration_wrapper)
            .await;
        coordinator
            .register("lifecycle".to_string(), lifecycle_wrapper)
            .await;
        coordinator
            .register("pubsub".to_string(), pubsub_wrapper)
            .await;
        coordinator
            .register("global_consensus".to_string(), global_consensus_wrapper)
            .await;
        coordinator
            .register("group_consensus".to_string(), group_consensus_wrapper)
            .await;
        coordinator
            .register("client".to_string(), client_wrapper)
            .await;

        // Wire up ClientService dependencies
        client_service
            .set_global_consensus(global_consensus_service.clone())
            .await;
        client_service
            .set_group_consensus(group_consensus_service.clone())
            .await;
        client_service
            .set_routing_service(routing_service.clone())
            .await;

        // Set start order
        coordinator
            .set_start_order(vec![
                "network".to_string(),
                "event".to_string(),
                "monitoring".to_string(),
                "global_consensus".to_string(),
                "group_consensus".to_string(),
                "cluster".to_string(),
                "routing".to_string(),
                "pubsub".to_string(),
                "client".to_string(),
                "migration".to_string(),
                "lifecycle".to_string(),
            ])
            .await;

        // Create engine
        let mut engine = Engine::new(
            self.node_id,
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
            network_manager,
            storage,
        );

        // Set consensus services
        engine.set_consensus_services(global_consensus_service, group_consensus_service);

        // Set the topology manager
        if let Some(ref topology) = self.topology_manager {
            engine.set_topology_manager(topology.clone());
        }

        // Now create engine handle wrapper and set it in the network service
        // This needs to be done after engine construction but before starting services
        // We'll add a method to Engine to set this up

        Ok(engine)
    }
}

/// Service wrapper to adapt services to ServiceLifecycle trait
struct ServiceWrapper<S> {
    name: String,
    service: Arc<S>,
}

impl<S> ServiceWrapper<S> {
    fn new(name: &str, service: Arc<S>) -> Self {
        Self {
            name: name.to_string(),
            service,
        }
    }
}

// Implement ServiceLifecycle for each service wrapper
use crate::foundation::traits::{HealthStatus, ServiceHealth};
use async_trait::async_trait;

#[async_trait]
impl<T, G> ServiceLifecycle for ServiceWrapper<ClusterService<T, G>>
where
    T: Transport + 'static,
    G: Governance + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await.map_err(|e| e.into())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await.map_err(|e| e.into())
    }

    async fn is_running(&self) -> bool {
        // ClusterService would need an is_running method
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        let health = self
            .service
            .get_health()
            .await
            .map_err(ConsensusError::from)?;

        Ok(ServiceHealth {
            name: self.name.clone(),
            status: match health.status {
                crate::services::cluster::HealthStatus::Healthy => HealthStatus::Healthy,
                crate::services::cluster::HealthStatus::Degraded => HealthStatus::Degraded,
                crate::services::cluster::HealthStatus::Unhealthy => HealthStatus::Unhealthy,
            },
            message: None,
            subsystems: Vec::new(),
        })
    }
}

#[async_trait]
impl ServiceLifecycle for ServiceWrapper<EventService> {
    async fn start(&self) -> ConsensusResult<()> {
        // Already started in builder
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await.map_err(|e| e.into())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for DiscoveryService

// Implement for MonitoringService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<MonitoringService> {
    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for RoutingService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<RoutingService> {
    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for MigrationService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<MigrationService> {
    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for LifecycleService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<LifecycleService> {
    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for NetworkService
#[async_trait]
impl<T, G> ServiceLifecycle for ServiceWrapper<NetworkService<T, G>>
where
    T: Transport + Send + Sync + 'static,
    G: Governance + Send + Sync + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        // Already started in builder
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        // NetworkService would need a stop method
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: HealthStatus::Healthy,
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for PubSubService
#[async_trait]
impl<T, G> ServiceLifecycle for ServiceWrapper<PubSubService<T, G>>
where
    T: Transport + Send + Sync + 'static,
    G: Governance + Send + Sync + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        // PubSubService needs to be mutable to start, so we'll handle this differently
        // For now, just return Ok as the service will be started separately
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        // PubSubService needs to be mutable to stop
        Ok(())
    }

    async fn is_running(&self) -> bool {
        true
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        match self.service.health_check().await {
            Ok(_) => Ok(ServiceHealth {
                name: self.name.clone(),
                status: HealthStatus::Healthy,
                message: None,
                subsystems: Vec::new(),
            }),
            Err(e) => Ok(ServiceHealth {
                name: self.name.clone(),
                status: HealthStatus::Unhealthy,
                message: Some(e.to_string()),
                subsystems: Vec::new(),
            }),
        }
    }
}

// Implement for GlobalConsensusService
#[async_trait]
impl<T, G, L> ServiceLifecycle
    for ServiceWrapper<crate::services::global_consensus::GlobalConsensusService<T, G, L>>
where
    T: Transport + Send + Sync + 'static,
    G: Governance + Send + Sync + 'static,
    L: LogStorage + Send + Sync + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_running(&self) -> bool {
        self.service.is_healthy().await
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: if self.service.is_healthy().await {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for GroupConsensusService
#[async_trait]
impl<T, G, L> ServiceLifecycle
    for ServiceWrapper<crate::services::group_consensus::GroupConsensusService<T, G, L>>
where
    T: Transport + Send + Sync + 'static,
    G: Governance + Send + Sync + 'static,
    L: LogStorage + Send + Sync + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_running(&self) -> bool {
        self.service.is_healthy().await
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        Ok(ServiceHealth {
            name: self.name.clone(),
            status: if self.service.is_healthy().await {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            message: None,
            subsystems: Vec::new(),
        })
    }
}

// Implement for ClientService
#[async_trait]
impl<T, G, L> ServiceLifecycle for ServiceWrapper<crate::services::client::ClientService<T, G, L>>
where
    T: Transport + Send + Sync + 'static,
    G: Governance + Send + Sync + 'static,
    L: LogStorage + Send + Sync + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_running(&self) -> bool {
        self.service.is_running().await
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        self.service.health_check().await
    }
}
