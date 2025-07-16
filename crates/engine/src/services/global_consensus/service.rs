//! Global consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::NodeId;
use proven_transport::Transport;

use super::config::{GlobalConsensusConfig, ServiceState};
use crate::{
    consensus::global::{GlobalConsensusLayer, raft::RaftMessageHandler},
    error::{ConsensusError, ConsensusResult, ErrorKind},
    services::{
        cluster::{ClusterFormationCallback, ClusterFormationEvent},
        event::{Event, EventPublisher},
    },
};

/// Global consensus service
pub struct GlobalConsensusService<T, G, L>
where
    T: Transport,
    G: Governance,
    L: LogStorage,
{
    /// Configuration
    config: GlobalConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage
    storage: L,
    /// Consensus layer (initialized after cluster formation)
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
    /// Service state
    state: Arc<RwLock<ServiceState>>,
    /// Topology manager
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
}

impl<T, G, L> GlobalConsensusService<T, G, L>
where
    T: Transport + 'static,
    G: Governance + 'static,
    L: LogStorage + 'static,
{
    /// Create new global consensus service
    pub fn new(
        config: GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage,
            consensus_layer: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ServiceState::NotInitialized)),
            topology_manager: None,
            event_publisher: None,
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
        if *state != ServiceState::NotInitialized {
            return Err(ConsensusError::with_context(
                ErrorKind::InvalidState,
                "Service already started",
            ));
        }
        *state = ServiceState::Initializing;

        // Register consensus message handlers
        self.register_message_handlers().await?;

        *state = ServiceState::Running;
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;
        Ok(())
    }

    /// Get cluster formation callback
    pub fn get_formation_callback(&self) -> ClusterFormationCallback {
        let consensus_layer = self.consensus_layer.clone();
        let config = self.config.clone();
        let node_id = self.node_id.clone();
        let network_manager = self.network_manager.clone();
        let storage = self.storage.clone();
        let topology_manager = self.topology_manager.clone();
        let event_publisher = self.event_publisher.clone();

        Arc::new(move |event| {
            let consensus_layer = consensus_layer.clone();
            let config = config.clone();
            let node_id = node_id.clone();
            let network_manager = network_manager.clone();
            let storage = storage.clone();
            let topology_manager = topology_manager.clone();
            let event_publisher = event_publisher.clone();

            Box::pin(async move {
                let result = async {
                    match event {
                        ClusterFormationEvent::FormedAsLeader { cluster_id, members } => {
                            tracing::info!("GlobalConsensusService: Forming consensus as leader of cluster {}", cluster_id);

                            // Create and initialize consensus layer
                            let layer = Self::create_consensus_layer(
                                &config,
                                node_id.clone(),
                                network_manager,
                                storage,
                            ).await?;

                            // Get member nodes from topology
                            let topology = topology_manager.ok_or_else(||
                                ConsensusError::with_context(ErrorKind::Configuration, "No topology manager")
                            )?;

                            let all_nodes = topology.provider()
                                .get_topology()
                                .await
                                .map_err(|e| ConsensusError::with_context(
                                    ErrorKind::Configuration,
                                    format!("Failed to get topology: {e}")
                                ))?;

                            let mut raft_members = std::collections::BTreeMap::new();
                            for member in &members {
                                if let Some(node) = all_nodes.iter().find(|n| NodeId::new(n.public_key) == *member) {
                                    raft_members.insert(member.clone(), node.clone());
                                }
                            }

                            // Initialize cluster
                            let handler: &dyn RaftMessageHandler = layer.as_ref();
                            handler.initialize_cluster(raft_members).await?;

                            let mut consensus_guard = consensus_layer.write().await;
                            *consensus_guard = Some(layer);

                            // Emit event
                            if let Some(ref publisher) = event_publisher {
                                let _ = publisher.publish(Event::GlobalConsensusInitialized {
                                    node_id: node_id.clone(),
                                    members: members.clone(),
                                }, "global_consensus".to_string()).await;
                            }
                        }
                        ClusterFormationEvent::JoinedAsFollower { cluster_id, .. } => {
                            tracing::info!("GlobalConsensusService: Joining consensus as follower in cluster {}", cluster_id);

                            // Create consensus layer as follower
                            let layer = Self::create_consensus_layer(
                                &config,
                                node_id.clone(),
                                network_manager,
                                storage,
                            ).await?;

                            let mut consensus_guard = consensus_layer.write().await;
                            *consensus_guard = Some(layer);

                            // Emit event
                            if let Some(ref publisher) = event_publisher {
                                let _ = publisher.publish(Event::GlobalConsensusInitialized {
                                    node_id: node_id.clone(),
                                    members: vec![node_id.clone()], // As follower, we don't know all members yet
                                }, "global_consensus".to_string()).await;
                            }
                        }
                        ClusterFormationEvent::FormationFailed { error } => {
                            tracing::error!("GlobalConsensusService: Cluster formation failed: {}", error);
                        }
                    }
                    Ok::<(), ConsensusError>(())
                }.await;

                if let Err(e) = result {
                    tracing::error!(
                        "GlobalConsensusService: Error handling formation event: {}",
                        e
                    );
                }
            })
        })
    }

    /// Create consensus layer
    async fn create_consensus_layer(
        config: &GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
    ) -> ConsensusResult<Arc<GlobalConsensusLayer<L>>> {
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

        let layer =
            GlobalConsensusLayer::new(node_id, raft_config, network_factory, storage).await?;

        Ok(Arc::new(layer))
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        use super::messages::GLOBAL_CONSENSUS_NAMESPACE;

        // Register namespace
        self.network_manager
            .register_namespace(GLOBAL_CONSENSUS_NAMESPACE)
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        // Register handlers for each global consensus message type
        use super::messages::*;

        // Register vote request handler
        let consensus_layer = self.consensus_layer.clone();
        self.network_manager
            .register_namespaced_request_handler::<GlobalVoteRequest, GlobalVoteResponse, _, _>(
                GLOBAL_CONSENSUS_NAMESPACE,
                "vote_request",
                move |_sender, message| {
                    let consensus_layer = consensus_layer.clone();
                    async move {
                        let layer_guard = consensus_layer.read().await;
                        let layer = layer_guard.as_ref().ok_or_else(|| {
                            proven_network::NetworkError::Other(
                                "Consensus not initialized".to_string(),
                            )
                        })?;

                        let handler: &dyn RaftMessageHandler = layer.as_ref();
                        let resp = handler
                            .handle_vote(message.0)
                            .await
                            .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                        Ok(GlobalVoteResponse(resp))
                    }
                },
            )
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        // Register append entries handler
        let consensus_layer = self.consensus_layer.clone();
        self.network_manager
            .register_namespaced_request_handler::<GlobalAppendEntriesRequest, GlobalAppendEntriesResponse, _, _>(
                GLOBAL_CONSENSUS_NAMESPACE,
                "append_entries_request",
                move |_sender, message| {
                    let consensus_layer = consensus_layer.clone();
                    async move {
                        let layer_guard = consensus_layer.read().await;
                        let layer = layer_guard.as_ref().ok_or_else(||
                            proven_network::NetworkError::Other("Consensus not initialized".to_string())
                        )?;

                        let handler: &dyn RaftMessageHandler = layer.as_ref();
                        let resp = handler.handle_append_entries(message.0).await
                            .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                        Ok(GlobalAppendEntriesResponse(resp))
                    }
                },
            )
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        // Register install snapshot handler
        let consensus_layer = self.consensus_layer.clone();
        self.network_manager
            .register_namespaced_request_handler::<GlobalInstallSnapshotRequest, GlobalInstallSnapshotResponse, _, _>(
                GLOBAL_CONSENSUS_NAMESPACE,
                "install_snapshot_request",
                move |_sender, message| {
                    let consensus_layer = consensus_layer.clone();
                    async move {
                        let layer_guard = consensus_layer.read().await;
                        let layer = layer_guard.as_ref().ok_or_else(||
                            proven_network::NetworkError::Other("Consensus not initialized".to_string())
                        )?;

                        let handler: &dyn RaftMessageHandler = layer.as_ref();
                        let resp = handler.handle_install_snapshot(message.0).await
                            .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                        Ok(GlobalInstallSnapshotResponse(resp))
                    }
                },
            )
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }
}
