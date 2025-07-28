//! Global consensus service
//!
//! This service manages the global Raft consensus layer that maintains cluster-wide state including:
//! - Consensus group definitions
//! - Stream metadata
//! - Cluster membership
//!
//! The service provides:
//! - Cluster initialization and membership management
//! - Request submission to the consensus layer
//! - Command handlers for external services to interact with global state

use proven_attestation::Attestor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::{ConsensusStorage, LogStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use tracing::{debug, info, warn};

use super::{
    callbacks::GlobalConsensusCallbacksImpl,
    config::{GlobalConsensusConfig, ServiceState},
};
use crate::foundation::routing::RoutingTable;
use crate::foundation::state::access::GlobalStateRead;
use crate::foundation::{GlobalStateReader, GlobalStateWriter, create_state_access};
use crate::{
    consensus::global::{GlobalConsensusCallbacks, GlobalConsensusLayer},
    error::{ConsensusResult, Error, ErrorKind},
    foundation::events::EventBus,
    foundation::types::ConsensusGroupId,
};

/// Type alias for the consensus layer storage
type ConsensusLayer<S> = Arc<RwLock<Option<Arc<GlobalConsensusLayer<ConsensusStorage<S>>>>>>;

/// Global consensus service
pub struct GlobalConsensusService<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
    A: Attestor,
{
    /// Configuration
    config: GlobalConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G, A>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Global state (read-only access)
    global_state: Arc<RwLock<Option<GlobalStateReader>>>,
    /// Consensus layer (initialized after cluster formation)
    consensus_layer: ConsensusLayer<S>,
    /// Service state
    state: Arc<RwLock<ServiceState>>,
    /// Topology manager
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
}

impl<T, G, A, S> GlobalConsensusService<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
    A: Attestor + 'static,
{
    /// Create new global consensus service
    pub fn new(
        config: GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage_manager: Arc<StorageManager<S>>,
        event_bus: Arc<EventBus>,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            global_state: Arc::new(RwLock::new(None)),
            consensus_layer: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ServiceState::NotInitialized)),
            topology_manager: None,
            event_bus,
            routing_table,
        }
    }

    /// Set topology manager
    pub fn with_topology(mut self, topology: Arc<proven_topology::TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology);
        self
    }

    /// Start the service
    pub async fn start(self: Arc<Self>) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotInitialized | ServiceState::Stopped => {
                *state = ServiceState::Initializing;
            }
            _ => {
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Service already started",
                ));
            }
        }

        let (state_reader, state_writer) = create_state_access();
        *self.global_state.write().await = Some(state_reader.clone());

        // Create consensus layer early to handle messages
        if self.consensus_layer.read().await.is_none() {
            let consensus_storage = self.storage_manager.consensus_storage();
            let callbacks = Arc::new(GlobalConsensusCallbacksImpl::new(
                self.node_id.clone(),
                Some(self.event_bus.clone()),
                state_reader.clone(),
                self.routing_table.clone(),
            ));

            let layer = Self::create_consensus_layer(
                &self.config,
                self.node_id.clone(),
                self.network_manager.clone(),
                consensus_storage,
                callbacks.clone(),
                state_writer,
            )
            .await?;

            // Store the layer
            {
                let mut guard = self.consensus_layer.write().await;
                *guard = Some(layer.clone());
            }

            // Start leader monitoring
            layer.start_leader_monitoring(callbacks);

            // Register message handlers
            Self::register_message_handlers_static(
                self.consensus_layer.clone(),
                self.network_manager.clone(),
            )
            .await?;
        }

        // Register command handlers
        self.register_command_handlers().await;

        *state = ServiceState::Running;

        // Start background task to ensure default group exists
        self.start_default_group_monitor();

        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;
        drop(state);

        // Shutdown the Raft instance if it exists
        let mut consensus_layer = self.consensus_layer.write().await;
        if let Some(layer) = consensus_layer.as_ref() {
            // Shutdown the Raft instance to release all resources with timeout
            match tokio::time::timeout(std::time::Duration::from_secs(5), layer.shutdown()).await {
                Ok(Ok(())) => {
                    tracing::debug!("Global Raft instance shut down successfully");
                }
                Ok(Err(e)) => {
                    tracing::error!("Failed to shutdown global Raft instance: {}", e);
                }
                Err(_) => {
                    tracing::error!("Timeout while shutting down global Raft instance");
                }
            }
        }

        // Clear the consensus layer to release all references
        *consensus_layer = None;
        drop(consensus_layer);
        tracing::debug!("Consensus layer cleared and lock released");

        // Unregister from network service
        let _ = self
            .network_manager
            .unregister_service("global_consensus")
            .await;

        // Unregister all event handlers to allow re-registration on restart
        use crate::services::global_consensus::commands::*;
        let _ = self
            .event_bus
            .unregister_request_handler::<SubmitGlobalRequest>();
        let _ = self
            .event_bus
            .unregister_request_handler::<InitializeGlobalConsensus>();
        let _ = self
            .event_bus
            .unregister_request_handler::<AddNodeToConsensus>();
        let _ = self
            .event_bus
            .unregister_request_handler::<AddNodeToGroup>();
        let _ = self
            .event_bus
            .unregister_request_handler::<RemoveNodeFromConsensus>();
        let _ = self
            .event_bus
            .unregister_request_handler::<GetGlobalConsensusMembers>();
        let _ = self
            .event_bus
            .unregister_request_handler::<UpdateGlobalMembership>();
        let _ = self.event_bus.unregister_request_handler::<CreateStream>();
        let _ = self
            .event_bus
            .unregister_request_handler::<GetGlobalState>();

        tracing::debug!("GlobalConsensusService stop completed");
        Ok(())
    }

    /// Create consensus layer
    async fn create_consensus_layer<L>(
        config: &GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage: L,
        callbacks: Arc<dyn GlobalConsensusCallbacks>,
        state_writer: GlobalStateWriter,
    ) -> ConsensusResult<Arc<GlobalConsensusLayer<L>>>
    where
        L: LogStorage + 'static,
    {
        use super::adaptor::GlobalNetworkFactory;
        use openraft::Config;

        let raft_config = Config {
            cluster_name: format!("global-{node_id}"),
            election_timeout_min: config.election_timeout_min.as_millis() as u64,
            election_timeout_max: config.election_timeout_max.as_millis() as u64,
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            max_payload_entries: config.max_entries_per_append as u64,
            // TODO: Think about snapshotting
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        };

        let network_factory = GlobalNetworkFactory::new(network_manager);

        let layer = GlobalConsensusLayer::new(
            node_id.clone(),
            raft_config,
            network_factory,
            storage,
            callbacks,
            state_writer,
        )
        .await?;

        Ok(Arc::new(layer))
    }

    /// Register message handlers
    async fn register_message_handlers_static(
        consensus_layer: ConsensusLayer<S>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> ConsensusResult<()> {
        use super::handler::GlobalConsensusHandler;

        // Create and register the service handler
        let handler = GlobalConsensusHandler::new(consensus_layer);
        network_manager
            .register_service(handler)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Start background task to ensure default group exists
    fn start_default_group_monitor(&self) {
        let consensus_layer = self.consensus_layer.clone();
        let global_state = self.global_state.clone();

        tokio::spawn(async move {
            info!("Starting default group monitor task");

            // Wait for leadership to be established first
            let mut leadership_check_interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                leadership_check_interval.tick().await;
                let consensus_guard = consensus_layer.read().await;
                if let Some(consensus) = consensus_guard.as_ref()
                    && consensus.get_leader().is_some()
                {
                    drop(consensus_guard);
                    info!("Leadership established, proceeding with default group creation");
                    break;
                }
            }

            // Now check for creating default group
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            let default_group_id = ConsensusGroupId::new(1);

            loop {
                interval.tick().await;

                // Check if default group exists
                let state_guard = global_state.read().await;
                if let Some(state) = state_guard.as_ref()
                    && state.get_group(&default_group_id).await.is_some()
                {
                    debug!("Default group exists, stopping monitor");
                    break;
                }
                drop(state_guard);

                // Check if we're the leader
                let consensus_guard = consensus_layer.read().await;
                if let Some(consensus) = consensus_guard.as_ref() {
                    if consensus.is_leader().await {
                        // Get current members
                        let members = consensus.get_members();

                        info!(
                            "We are the leader with {} members, creating default group",
                            members.len()
                        );

                        // Create default group request
                        let create_group_request =
                            crate::consensus::global::GlobalRequest::CreateGroup {
                                info: crate::foundation::GroupInfo {
                                    id: default_group_id,
                                    members: members.clone(),
                                    created_at: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis()
                                        as u64,
                                    metadata: Default::default(),
                                },
                            };

                        match consensus.submit_request(create_group_request).await {
                            Ok(_) => {
                                info!("Successfully created default group");
                            }
                            Err(e) => {
                                warn!("Failed to create default group: {}", e);
                            }
                        }
                    } else {
                        debug!("Not the leader, waiting...");
                    }
                } else {
                    debug!("Consensus layer not initialized yet");
                }
            }

            info!("Default group monitor task completed");
        });
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    /// Register command handlers for global consensus
    async fn register_command_handlers(&self) {
        use crate::services::global_consensus::command_handlers::*;
        use crate::services::global_consensus::commands::*;

        // Extract the specific dependencies needed by handlers
        let consensus_layer = self.consensus_layer.clone();
        let network_manager = self.network_manager.clone();
        let routing_table = self.routing_table.clone();
        let event_bus = self.event_bus.clone();

        // Register SubmitGlobalRequest handler
        let submit_handler =
            SubmitGlobalRequestHandler::new(consensus_layer.clone(), network_manager.clone());
        event_bus
            .handle_requests::<SubmitGlobalRequest, _>(submit_handler)
            .expect("Failed to register SubmitGlobalRequest handler");

        // Register InitializeGlobalConsensus handler
        let init_handler = InitializeGlobalConsensusHandler::new(consensus_layer.clone());
        event_bus
            .handle_requests::<InitializeGlobalConsensus, _>(init_handler)
            .expect("Failed to register InitializeGlobalConsensus handler");

        // Register AddNodeToConsensus handler (for adding nodes to global Raft consensus)
        let topology_manager = self
            .topology_manager
            .clone()
            .expect("Topology manager must be set before registering command handlers");
        let add_node_handler =
            AddNodeToConsensusHandler::new(consensus_layer.clone(), topology_manager);
        event_bus
            .handle_requests::<AddNodeToConsensus, _>(add_node_handler)
            .expect("Failed to register AddNodeToConsensus handler");

        // Register AddNodeToGroup handler (for adding nodes to consensus groups)
        let add_to_group_handler = AddNodeToGroupHandler::new(consensus_layer.clone());
        event_bus
            .handle_requests::<AddNodeToGroup, _>(add_to_group_handler)
            .expect("Failed to register AddNodeToGroup handler");

        // Register RemoveNodeFromConsensus handler
        let remove_node_handler = RemoveNodeFromGroupHandler::new(consensus_layer.clone());
        event_bus
            .handle_requests::<RemoveNodeFromConsensus, _>(remove_node_handler)
            .expect("Failed to register RemoveNodeFromConsensus handler");

        // Register GetGlobalConsensusMembers handler
        let get_members_handler = GetGlobalConsensusMembersHandler::new(consensus_layer.clone());
        event_bus
            .handle_requests::<GetGlobalConsensusMembers, _>(get_members_handler)
            .expect("Failed to register GetGlobalConsensusMembers handler");

        // Register UpdateGlobalMembership handler
        let update_membership_handler = UpdateGlobalMembershipHandler::new(consensus_layer.clone());
        event_bus
            .handle_requests::<UpdateGlobalMembership, _>(update_membership_handler)
            .expect("Failed to register UpdateGlobalMembership handler");

        // Register CreateStream handler
        let create_stream_handler =
            CreateStreamHandler::new(consensus_layer.clone(), routing_table, event_bus.clone());
        event_bus
            .handle_requests::<CreateStream, _>(create_stream_handler)
            .expect("Failed to register CreateStream handler");

        // Register GetGlobalState handler
        let get_state_handler = GetGlobalStateHandler::new(self.global_state.clone());
        event_bus
            .handle_requests::<GetGlobalState, _>(get_state_handler)
            .expect("Failed to register GetGlobalState handler");
    }
}
