//! Global consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::{ConsensusStorage, LogStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::config::{GlobalConsensusConfig, ServiceState};
use crate::{
    consensus::global::{GlobalConsensusLayer, raft::GlobalRaftMessageHandler},
    error::{ConsensusError, ConsensusResult, ErrorKind},
    services::{
        cluster::{ClusterFormationCallback, ClusterFormationEvent},
        event::{Event, EventPublisher},
    },
};

/// Type alias for the consensus layer storage
type ConsensusLayer<S> = Arc<RwLock<Option<Arc<GlobalConsensusLayer<ConsensusStorage<S>>>>>>;

/// Global consensus service
pub struct GlobalConsensusService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Configuration
    config: GlobalConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Consensus layer (initialized after cluster formation)
    consensus_layer: ConsensusLayer<S>,
    /// Service state
    state: Arc<RwLock<ServiceState>>,
    /// Topology manager
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
    /// Track if handlers have been registered
    handlers_registered: Arc<RwLock<bool>>,
}

impl<T, G, S> GlobalConsensusService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create new global consensus service
    pub fn new(
        config: GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            consensus_layer: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ServiceState::NotInitialized)),
            topology_manager: None,
            event_publisher: None,
            handlers_registered: Arc::new(RwLock::new(false)),
        }
    }

    /// Set topology manager
    pub fn with_topology(mut self, topology: Arc<proven_topology::TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology);
        self
    }

    /// Set event publisher
    pub fn with_event_publisher(mut self, publisher: EventPublisher) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotInitialized | ServiceState::Stopped => {
                *state = ServiceState::Initializing;
            }
            _ => {
                return Err(ConsensusError::with_context(
                    ErrorKind::InvalidState,
                    "Service already started",
                ));
            }
        }

        // Don't register handlers here - they will be registered after consensus layer is created
        // self.register_message_handlers().await?;

        *state = ServiceState::Running;
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;
        drop(state);

        // First, unregister the service handler from NetworkManager
        // Do this before shutting down Raft to avoid potential deadlocks
        use super::messages::GlobalConsensusMessage;
        tracing::debug!("Unregistering global consensus service handler");
        if let Err(e) = self
            .network_manager
            .unregister_service::<GlobalConsensusMessage>()
            .await
        {
            tracing::warn!("Failed to unregister global consensus service: {}", e);
        } else {
            tracing::debug!("Global consensus service handler unregistered");
        }

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

        tracing::debug!("GlobalConsensusService stop completed");
        Ok(())
    }

    /// Get cluster formation callback
    pub fn get_formation_callback(&self) -> ClusterFormationCallback {
        let consensus_layer = self.consensus_layer.clone();
        let config = self.config.clone();
        let node_id = self.node_id.clone();
        let network_manager = self.network_manager.clone();
        let storage_manager = self.storage_manager.clone();
        let topology_manager = self.topology_manager.clone();
        let event_publisher = self.event_publisher.clone();

        Arc::new(move |event| {
            let consensus_layer = consensus_layer.clone();
            let config = config.clone();
            let node_id = node_id.clone();
            let network_manager = network_manager.clone();
            let storage_manager = storage_manager.clone();
            let topology_manager = topology_manager.clone();
            let event_publisher = event_publisher.clone();

            Box::pin(async move {
                match event {
                    ClusterFormationEvent::FormedAsLeader {
                        cluster_id,
                        members,
                    } => {
                        tracing::info!(
                            "GlobalConsensusService: Forming consensus as leader of cluster {}",
                            cluster_id
                        );

                        // Create and initialize consensus layer
                        // Get consensus storage view for global consensus
                        let consensus_storage = storage_manager.consensus_storage();
                        let layer = match Self::create_consensus_layer(
                            &config,
                            node_id.clone(),
                            network_manager.clone(),
                            consensus_storage,
                            event_publisher.clone(),
                        )
                        .await
                        {
                            Ok(l) => l,
                            Err(e) => {
                                tracing::error!("Failed to create consensus layer: {}", e);
                                return;
                            }
                        };

                        // Get member nodes from topology
                        let topology = match topology_manager {
                            Some(tm) => tm,
                            None => {
                                tracing::error!("No topology manager");
                                return;
                            }
                        };

                        let all_nodes = match topology.provider().get_topology().await {
                            Ok(nodes) => nodes,
                            Err(e) => {
                                tracing::error!("Failed to get topology: {}", e);
                                return;
                            }
                        };

                        let mut raft_members = std::collections::BTreeMap::new();
                        for member in &members {
                            if let Some(node) = all_nodes.iter().find(|n| n.node_id == *member) {
                                raft_members.insert(member.clone(), node.clone());
                            }
                        }

                        // Initialize cluster
                        let handler: &dyn GlobalRaftMessageHandler = layer.as_ref();
                        if let Err(e) = handler.initialize_cluster(raft_members).await {
                            tracing::error!("Failed to initialize cluster: {}", e);
                            return;
                        }

                        // Event publisher already set during layer creation

                        // Store the layer clone for later use
                        let layer_clone = layer.clone();
                        let mut consensus_guard = consensus_layer.write().await;
                        *consensus_guard = Some(layer);
                        drop(consensus_guard); // Release the lock before registering handlers

                        // Register message handlers now that consensus layer exists
                        if let Err(e) = Self::register_message_handlers_static(
                            consensus_layer.clone(),
                            network_manager.clone(),
                        )
                        .await
                        {
                            tracing::error!(
                                "Failed to register global consensus message handlers: {}",
                                e
                            );
                            return;
                        }

                        // Emit event
                        if let Some(ref publisher) = event_publisher {
                            let _ = publisher
                                .publish(
                                    Event::GlobalConsensusInitialized {
                                        node_id: node_id.clone(),
                                        members: members.clone(),
                                    },
                                    "global_consensus".to_string(),
                                )
                                .await;
                        }

                        // Create default group through global consensus
                        tracing::info!(
                            "GlobalConsensusService: Creating default group through consensus"
                        );
                        // Spawn a task to create the default group after a small delay
                        // This ensures Raft has time to stabilize after initialization
                        tokio::spawn(async move {
                            // Wait a bit for Raft to stabilize
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            // Submit CreateGroup request to global consensus
                            let create_group_request =
                                crate::consensus::global::GlobalRequest::CreateGroup {
                                    info: crate::consensus::global::types::GroupInfo {
                                        id: crate::foundation::types::ConsensusGroupId::new(1),
                                        members: members.clone(),
                                        created_at: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs(),
                                        metadata: std::collections::HashMap::new(),
                                    },
                                };
                            // Submit the request to create the default group using the clone
                            match layer_clone.submit_request(create_group_request).await {
                                Ok(_) => {
                                    tracing::info!(
                                        "Successfully submitted default group creation to consensus"
                                    );
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to create default group through consensus: {}",
                                        e
                                    );
                                }
                            }
                        });
                    }
                    ClusterFormationEvent::JoinedAsFollower { cluster_id, .. } => {
                        tracing::info!(
                            "GlobalConsensusService: Joining consensus as follower in cluster {}",
                            cluster_id
                        );

                        // Create consensus layer as follower
                        // Get consensus storage view for global consensus
                        let consensus_storage = storage_manager.consensus_storage();
                        let layer = match Self::create_consensus_layer(
                            &config,
                            node_id.clone(),
                            network_manager.clone(),
                            consensus_storage,
                            event_publisher.clone(),
                        )
                        .await
                        {
                            Ok(l) => l,
                            Err(e) => {
                                tracing::error!("Failed to create consensus layer: {}", e);
                                return;
                            }
                        };

                        // Event publisher already set during layer creation

                        let mut consensus_guard = consensus_layer.write().await;
                        *consensus_guard = Some(layer);
                        drop(consensus_guard); // Release the lock before registering handlers

                        // Register message handlers now that consensus layer exists
                        if let Err(e) = Self::register_message_handlers_static(
                            consensus_layer.clone(),
                            network_manager.clone(),
                        )
                        .await
                        {
                            tracing::error!(
                                "Failed to register global consensus message handlers: {}",
                                e
                            );
                            return;
                        }

                        // Emit event
                        if let Some(ref publisher) = event_publisher {
                            let _ = publisher
                                .publish(
                                    Event::GlobalConsensusInitialized {
                                        node_id: node_id.clone(),
                                        members: vec![node_id.clone()], // As follower, we don't know all members yet
                                    },
                                    "global_consensus".to_string(),
                                )
                                .await;
                        }
                    }
                    ClusterFormationEvent::FormationFailed { error } => {
                        tracing::error!(
                            "GlobalConsensusService: Cluster formation failed: {}",
                            error
                        );
                    }
                }
            })
        })
    }

    /// Create consensus layer
    async fn create_consensus_layer<L>(
        config: &GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
        event_publisher: Option<EventPublisher>,
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
            max_payload_entries: config.max_entries_per_append,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(config.snapshot_interval),
            ..Default::default()
        };

        let network_stats = Arc::new(RwLock::new(Default::default()));
        let network_factory = GlobalNetworkFactory::new(network_manager, network_stats);

        let mut layer =
            GlobalConsensusLayer::new(node_id.clone(), raft_config, network_factory, storage)
                .await?;

        // Set event publisher if provided
        if let Some(publisher) = event_publisher {
            layer.set_event_publisher(publisher).await;
        }

        Ok(Arc::new(layer))
    }

    /// Register message handlers
    async fn register_message_handlers_static(
        consensus_layer: ConsensusLayer<S>,
        network_manager: Arc<NetworkManager<T, G>>,
    ) -> ConsensusResult<()> {
        use super::messages::{GlobalConsensusMessage, GlobalConsensusResponse};
        use crate::consensus::global::raft::GlobalRaftMessageHandler;

        // Register the service handler
        network_manager
            .register_service::<GlobalConsensusMessage, _>(move |_sender, message| {
                let consensus_layer = consensus_layer.clone();
                Box::pin(async move {
                    let layer_guard = consensus_layer.read().await;
                    let layer = layer_guard.as_ref().ok_or_else(|| {
                        proven_network::NetworkError::Other("Consensus not initialized".to_string())
                    })?;

                    let handler: &dyn GlobalRaftMessageHandler = layer.as_ref();

                    match message {
                        GlobalConsensusMessage::Vote(req) => {
                            let resp = handler
                                .handle_vote(req)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GlobalConsensusResponse::Vote(resp))
                        }
                        GlobalConsensusMessage::AppendEntries(req) => {
                            let resp = handler
                                .handle_append_entries(req)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GlobalConsensusResponse::AppendEntries(resp))
                        }
                        GlobalConsensusMessage::InstallSnapshot(req) => {
                            let resp = handler
                                .handle_install_snapshot(req)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GlobalConsensusResponse::InstallSnapshot(resp))
                        }
                        GlobalConsensusMessage::Consensus(req) => {
                            let resp = layer
                                .submit_request(req)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GlobalConsensusResponse::Consensus(resp))
                        }
                    }
                })
            })
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Submit a request to global consensus
    pub async fn submit_request(
        &self,
        request: crate::consensus::global::GlobalRequest,
    ) -> ConsensusResult<crate::consensus::global::GlobalResponse> {
        // Check if consensus layer is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                ErrorKind::InvalidState,
                "Global consensus not initialized",
            )
        })?;

        // Submit to consensus
        consensus.submit_request(request).await
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }
}
