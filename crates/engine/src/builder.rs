//! Engine builder pattern

use std::sync::Arc;

use proven_network::NetworkManager;
use proven_storage::{LogStorage, LogStorageWithDelete, StorageAdaptor, StorageManager};
use proven_topology::TopologyAdaptor;
use proven_topology::{NodeId, TopologyManager};
use proven_transport::Transport;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::traits::{ServiceLifecycle, lifecycle::ServiceStatus};
use crate::services::{
    client::ClientService,
    event::{EventService, EventServiceConfig},
    lifecycle::LifecycleService,
    migration::MigrationService,
    monitoring::MonitoringService,
    pubsub::PubSubService,
    routing::RoutingService,
};
use tracing::{error, info};

use super::config::EngineConfig;
use super::coordinator::ServiceCoordinator;
use super::engine::Engine;

/// Engine builder
pub struct EngineBuilder<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,

    /// Configuration
    config: Option<EngineConfig>,

    /// Network manager
    network_manager: Option<Arc<NetworkManager<T, G>>>,

    /// Topology manager
    topology_manager: Option<Arc<TopologyManager<G>>>,

    /// Storage manager
    storage_manager: Option<Arc<StorageManager<S>>>,
}

impl<T, G, S> EngineBuilder<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new engine builder
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            config: None,
            network_manager: None,
            topology_manager: None,
            storage_manager: None,
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

    /// Set storage manager
    pub fn with_storage(mut self, storage_manager: Arc<StorageManager<S>>) -> Self {
        self.storage_manager = Some(storage_manager);
        self
    }

    /// Build the engine
    pub async fn build(self) -> ConsensusResult<Engine<T, G, S>> {
        // Validate required fields
        let config = self
            .config
            .ok_or_else(|| Error::with_context(ErrorKind::Configuration, "Config not set"))?;

        let network_manager = self.network_manager.ok_or_else(|| {
            Error::with_context(ErrorKind::Configuration, "Network manager not set")
        })?;

        let topology_manager = self.topology_manager.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Configuration, "Topology manager not set")
        })?;

        let storage_manager = self.storage_manager.ok_or_else(|| {
            Error::with_context(ErrorKind::Configuration, "Storage manager not set")
        })?;

        // Create service coordinator
        let coordinator = Arc::new(ServiceCoordinator::new());

        // Create other services
        let event_config = EventServiceConfig::default();
        let event_service = Arc::new(EventService::new(event_config));
        event_service.start().await?;

        // Create consensus services
        use crate::services::global_consensus::{GlobalConsensusConfig, GlobalConsensusService};
        use crate::services::group_consensus::{GroupConsensusConfig, GroupConsensusService};
        use crate::services::membership::{MembershipConfig, MembershipService};

        // Create membership service first as consensus services depend on it
        let membership_config = MembershipConfig::default();
        let node_info = topology_manager.get_own_node().await.map_err(|e| {
            Error::with_context(
                ErrorKind::Configuration,
                format!("Failed to get current node: {e}"),
            )
        })?;

        let membership_service = Arc::new(MembershipService::new(
            membership_config,
            self.node_id.clone(),
            node_info,
            network_manager.clone(),
            topology_manager.clone(),
            event_service.bus(),
        ));

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
                storage_manager.clone(),
                event_service.bus(),
            )
            .with_topology(topology_manager.clone()),
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
                storage_manager.clone(),
                event_service.bus(),
            )
            .with_topology(topology_manager.clone()),
        );

        // ClusterService has been removed - functionality moved to GlobalConsensusService

        // Create system view for monitoring
        let system_view = Arc::new(crate::services::monitoring::SystemView::new());
        let monitoring_service = Arc::new(MonitoringService::new(
            config.services.monitoring.clone(),
            self.node_id.clone(),
            system_view,
        ));

        let routing_service = Arc::new(RoutingService::new(
            config.services.routing.clone(),
            self.node_id.clone(),
            event_service.bus(),
        ));

        let migration_service = Arc::new(MigrationService::new(config.services.migration.clone()));

        let lifecycle_service = Arc::new(LifecycleService::new(config.services.lifecycle.clone()));

        // Create ClientService with event bus
        let client_service = Arc::new(ClientService::new(
            self.node_id.clone(),
            event_service.bus(),
        ));

        // Create StreamService with storage manager
        use crate::services::stream::{StreamService, StreamServiceConfig};
        let stream_config = StreamServiceConfig::default();
        let stream_service = Arc::new(StreamService::new(
            stream_config,
            storage_manager.clone(),
            event_service.bus(),
        ));

        // Create service wrappers that implement ServiceLifecycle
        let membership_wrapper = Arc::new(ServiceWrapper::new(
            "membership",
            membership_service.clone(),
        ));
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
        let client_wrapper = Arc::new(ServiceWrapper::new("client", client_service.clone()));
        let stream_wrapper = Arc::new(ServiceWrapper::new("stream", stream_service.clone()));

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
            .register("membership".to_string(), membership_wrapper)
            .await;
        coordinator
            .register("event".to_string(), event_wrapper)
            .await;
        coordinator
            .register("monitoring".to_string(), monitoring_wrapper)
            .await;
        // ClusterService removed - functionality in GlobalConsensusService
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
            .register("global_consensus".to_string(), global_consensus_wrapper)
            .await;
        coordinator
            .register("group_consensus".to_string(), group_consensus_wrapper)
            .await;
        coordinator
            .register("client".to_string(), client_wrapper)
            .await;
        coordinator
            .register("stream".to_string(), stream_wrapper)
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
        client_service
            .set_stream_service(stream_service.clone())
            .await;
        client_service
            .set_network_manager(network_manager.clone())
            .await;

        // Wire up GroupConsensusService dependencies
        group_consensus_service
            .set_stream_service(stream_service.clone())
            .await;

        // Wire up GlobalConsensusService dependencies
        global_consensus_service
            .set_group_consensus_service(group_consensus_service.clone())
            .await;
        // Routing and stream services are no longer set directly on global consensus
        // They communicate through events instead
        global_consensus_service
            .set_membership_service(membership_service.clone())
            .await;

        // Create PubSub service with network manager and event bus
        let pubsub_service = Arc::new(
            PubSubService::new(
                config.services.pubsub.clone(),
                self.node_id.clone(),
                network_manager.clone(),
                event_service.bus(),
            )
            .await,
        );

        // Register network handlers
        if let Err(e) = pubsub_service
            .clone()
            .register_network_handlers(network_manager.clone())
            .await
        {
            error!("Failed to register PubSub network handlers: {}", e);
        }

        // Setup event handler
        pubsub_service.clone().setup_event_handler().await;

        let pubsub_wrapper = Arc::new(ServiceWrapper::new("pubsub", pubsub_service.clone()));

        // Register PubSub service with coordinator
        coordinator
            .register("pubsub".to_string(), pubsub_wrapper)
            .await;

        // Set start order
        coordinator
            .set_start_order(vec![
                "event".to_string(),
                "monitoring".to_string(),
                "routing".to_string(), // Start routing before consensus services
                "stream".to_string(),  // Start stream service before consensus
                "group_consensus".to_string(), // Start group_consensus before global_consensus
                "global_consensus".to_string(), // So it's ready to receive events
                "pubsub".to_string(),
                "client".to_string(),
                "migration".to_string(),
                "lifecycle".to_string(),
                "membership".to_string(), // Start membership last to kick off cluster formation
            ])
            .await;

        // Create engine
        let mut engine = Engine::new(
            self.node_id,
            config,
            coordinator,
            event_service,
            monitoring_service,
            routing_service,
            migration_service,
            lifecycle_service,
            pubsub_service,
            client_service,
            stream_service,
            network_manager,
            storage_manager,
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
use async_trait::async_trait;

// ClusterService removed - functionality moved to GlobalConsensusService

#[async_trait]
impl ServiceLifecycle for ServiceWrapper<EventService> {
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        // Already started in builder
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        true
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for DiscoveryService

// Implement for MonitoringService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<MonitoringService> {
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for RoutingService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<RoutingService> {
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        true // Routing service is always healthy if started
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for MigrationService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<MigrationService> {
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for LifecycleService
#[async_trait]
impl ServiceLifecycle for ServiceWrapper<LifecycleService> {
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for PubSubService
#[async_trait]
impl<T, G> ServiceLifecycle for ServiceWrapper<PubSubService<T, G>>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        // PubSubService needs to be mutable to start, so we'll handle this differently
        // For now, just return Ok as the service will be started separately
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        // PubSubService needs to be mutable to stop
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        true
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for GlobalConsensusService
#[async_trait]
impl<T, G, S> ServiceLifecycle
    for ServiceWrapper<crate::services::global_consensus::GlobalConsensusService<T, G, S>>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        self.service.is_healthy().await
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for GroupConsensusService
#[async_trait]
impl<T, G, S> ServiceLifecycle
    for ServiceWrapper<crate::services::group_consensus::GroupConsensusService<T, G, S>>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        self.service.is_healthy().await
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for ClientService
#[async_trait]
impl<T, G, S> ServiceLifecycle for ServiceWrapper<crate::services::client::ClientService<T, G, S>>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        true // Routing service is always healthy if started
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for StreamService
#[async_trait]
impl<S> ServiceLifecycle for ServiceWrapper<crate::services::stream::StreamService<S>>
where
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        self.service.initialize().await
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        true // Routing service is always healthy if started
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

// Implement for MembershipService
#[async_trait]
impl<T, G, S> ServiceLifecycle
    for ServiceWrapper<crate::services::membership::MembershipService<T, G, S>>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        self.service.start().await
    }

    async fn stop(&self) -> ConsensusResult<()> {
        self.service.stop().await
    }

    async fn is_healthy(&self) -> bool {
        self.service.is_healthy().await
    }

    async fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}
