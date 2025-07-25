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

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::StorageNamespace;
use proven_storage::{ConsensusStorage, LogStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use tracing::{debug, error, info, warn};

use super::{
    callbacks::GlobalConsensusCallbacksImpl,
    config::{GlobalConsensusConfig, ServiceState},
};
use crate::foundation::routing::RoutingTable;
use crate::foundation::state::access::GlobalStateRead;
use crate::foundation::{GlobalState, GlobalStateReader, GlobalStateWriter, create_state_access};
use crate::{
    consensus::global::{
        GlobalConsensusCallbacks, GlobalConsensusLayer, raft::GlobalRaftMessageHandler,
    },
    error::{ConsensusResult, Error, ErrorKind},
    foundation::events::EventBus,
    foundation::{GroupInfo, types::ConsensusGroupId},
    services::stream::StreamName,
    services::{
        global_consensus::events::GlobalConsensusEvent, group_consensus::GroupConsensusService,
    },
};

/// Type alias for the consensus layer storage
type ConsensusLayer<S> = Arc<RwLock<Option<Arc<GlobalConsensusLayer<ConsensusStorage<S>>>>>>;

/// Type alias for the group consensus service
type GroupConsensusServiceRef<T, G, S> = Arc<RwLock<Option<Arc<GroupConsensusService<T, G, S>>>>>;

/// Type alias for the stream service
type StreamServiceRef<S> = Arc<RwLock<Option<Arc<crate::services::stream::StreamService<S>>>>>;

/// Type alias for Raft metrics receiver
type RaftMetricsReceiver =
    tokio::sync::watch::Receiver<openraft::RaftMetrics<crate::consensus::global::GlobalTypeConfig>>;

/// Type alias for the membership service
type MembershipServiceRef<T, G, S> =
    Arc<RwLock<Option<Arc<crate::services::membership::MembershipService<T, G, S>>>>>;

/// Type alias for Raft metrics receiver ref
type RaftMetricsReceiverRef = Arc<RwLock<Option<RaftMetricsReceiver>>>;

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
    /// Group consensus service reference (set by external services for cross-service coordination)
    group_consensus_service: GroupConsensusServiceRef<T, G, S>,
    /// Stream service reference (set by external services for cross-service coordination)
    stream_service: StreamServiceRef<S>,
    /// Membership service reference (set by external services for cross-service coordination)
    membership_service: MembershipServiceRef<T, G, S>,
    /// Raft metrics receiver (kept alive for the service lifetime)
    raft_metrics_rx: RaftMetricsReceiverRef,
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
            group_consensus_service: Arc::new(RwLock::new(None)),
            stream_service: Arc::new(RwLock::new(None)),
            membership_service: Arc::new(RwLock::new(None)),
            raft_metrics_rx: Arc::new(RwLock::new(None)),
        }
    }

    /// Set topology manager
    pub fn with_topology(mut self, topology: Arc<proven_topology::TopologyManager<G>>) -> Self {
        self.topology_manager = Some(topology);
        self
    }

    /// Get topology manager
    pub fn topology_manager(&self) -> Option<Arc<proven_topology::TopologyManager<G>>> {
        self.topology_manager.as_ref().cloned()
    }

    /// Set group consensus service for direct group creation
    pub async fn set_group_consensus_service(
        &self,
        group_consensus_service: Arc<GroupConsensusService<T, G, S>>,
    ) {
        *self.group_consensus_service.write().await = Some(group_consensus_service);
    }

    /// Set stream service for stream restoration
    pub async fn set_stream_service(
        &self,
        stream_service: Arc<crate::services::stream::StreamService<S>>,
    ) {
        *self.stream_service.write().await = Some(stream_service);
    }

    /// Set membership service
    pub async fn set_membership_service(
        &self,
        membership_service: Arc<crate::services::membership::MembershipService<T, G, S>>,
    ) {
        *self.membership_service.write().await = Some(membership_service);
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
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

            // Store metrics receiver
            {
                let metrics_rx = layer.metrics();
                let mut metrics_guard = self.raft_metrics_rx.write().await;
                *metrics_guard = Some(metrics_rx);
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

        *state = ServiceState::Running;
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

        tracing::debug!("GlobalConsensusService stop completed");
        Ok(())
    }

    /// Create consensus layer
    async fn create_consensus_layer<L>(
        config: &GlobalConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
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
        network_manager: Arc<NetworkManager<T, G>>,
    ) -> ConsensusResult<()> {
        use super::messages::{
            CheckClusterExistsResponse, GlobalConsensusMessage, GlobalConsensusResponse,
        };
        use crate::consensus::global::raft::GlobalRaftMessageHandler;

        // Register the service handler
        network_manager
            .register_service::<GlobalConsensusMessage, _>(move |_sender, message| {
                let consensus_layer = consensus_layer.clone();
                Box::pin(async move {
                    match message {
                        GlobalConsensusMessage::CheckClusterExists(_req) => {
                            // Handle cluster exists check without requiring consensus layer
                            let layer_guard = consensus_layer.read().await;
                            let response = if let Some(layer) = layer_guard.as_ref() {
                                // We have a consensus layer, check its state
                                let metrics = layer.metrics();
                                let metrics_data = metrics.borrow();
                                let membership = &metrics_data.membership_config;

                                CheckClusterExistsResponse {
                                    cluster_exists: true,
                                    current_leader: metrics_data.current_leader.clone(),
                                    current_term: metrics_data.current_term,
                                    members: membership.membership().voter_ids().collect(),
                                }
                            } else {
                                // No consensus layer yet
                                CheckClusterExistsResponse {
                                    cluster_exists: false,
                                    current_leader: None,
                                    current_term: 0,
                                    members: vec![],
                                }
                            };
                            Ok(GlobalConsensusResponse::CheckClusterExists(response))
                        }
                        _ => {
                            // All other messages require consensus layer
                            let layer_guard = consensus_layer.read().await;
                            let layer = layer_guard.as_ref().ok_or_else(|| {
                                proven_network::NetworkError::Other(
                                    "Consensus not initialized".to_string(),
                                )
                            })?;

                            let handler: &dyn GlobalRaftMessageHandler = layer.as_ref();

                            match message {
                                GlobalConsensusMessage::Vote(req) => {
                                    let resp = handler.handle_vote(req).await.map_err(|e| {
                                        proven_network::NetworkError::Other(e.to_string())
                                    })?;
                                    Ok(GlobalConsensusResponse::Vote(resp))
                                }
                                GlobalConsensusMessage::AppendEntries(req) => {
                                    let resp =
                                        handler.handle_append_entries(req).await.map_err(|e| {
                                            proven_network::NetworkError::Other(e.to_string())
                                        })?;
                                    Ok(GlobalConsensusResponse::AppendEntries(resp))
                                }
                                GlobalConsensusMessage::InstallSnapshot(req) => {
                                    let resp = handler.handle_install_snapshot(req).await.map_err(
                                        |e| proven_network::NetworkError::Other(e.to_string()),
                                    )?;
                                    Ok(GlobalConsensusResponse::InstallSnapshot(resp))
                                }
                                GlobalConsensusMessage::Consensus(req) => {
                                    let resp = layer.submit_request(req).await.map_err(|e| {
                                        proven_network::NetworkError::Other(e.to_string())
                                    })?;
                                    Ok(GlobalConsensusResponse::Consensus(resp))
                                }
                                GlobalConsensusMessage::CheckClusterExists(_) => {
                                    // Already handled above
                                    unreachable!()
                                }
                            }
                        }
                    }
                })
            })
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

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
            Error::with_context(ErrorKind::InvalidState, "Global consensus not initialized")
        })?;

        // Submit to consensus
        let response = consensus.submit_request(request).await?;

        // Routing updates are now handled by callbacks, not here

        Ok(response)
    }

    /// Initialize the global consensus cluster
    pub async fn initialize_cluster(&self, members: Vec<NodeId>) -> ConsensusResult<()> {
        info!(
            "Initializing global consensus cluster with {} members",
            members.len()
        );

        // Check if we have a consensus layer
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Consensus layer not initialized",
                ));
            }
        };

        // Check if Raft is already initialized
        if consensus.is_initialized().await {
            info!("Global consensus is already initialized, skipping initialization");
            return Ok(());
        }

        // Get node information from topology manager
        let mut raft_members = std::collections::BTreeMap::new();

        if let Some(topology_mgr) = &self.topology_manager {
            // Get node information for each member
            for member_id in &members {
                if let Some(node_info) = topology_mgr.get_node(member_id).await {
                    raft_members.insert(member_id.clone(), node_info);
                } else {
                    return Err(Error::with_context(
                        ErrorKind::NotFound,
                        format!("Could not find node info for member {member_id}"),
                    ));
                }
            }
        } else {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                "No topology manager available to get node information",
            ));
        }

        // Initialize cluster
        use crate::consensus::global::raft::GlobalRaftMessageHandler;
        let handler: &dyn GlobalRaftMessageHandler = consensus.as_ref();
        handler.initialize_cluster(raft_members).await?;

        info!("Successfully initialized global consensus Raft cluster");

        // For single-node clusters, wait for the node to become leader
        if members.len() == 1 {
            let start = std::time::Instant::now();
            let timeout = std::time::Duration::from_secs(5);

            loop {
                if consensus.is_leader().await {
                    info!("Single node became leader after initialization");
                    break;
                }

                if start.elapsed() > timeout {
                    return Err(Error::with_context(
                        ErrorKind::Timeout,
                        "Timeout waiting for single node to become leader",
                    ));
                }

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // Create default group
        let default_group_id = ConsensusGroupId::new(1);

        // Check if default group already exists
        let global_state_guard = self.global_state.read().await;
        if let Some(state) = global_state_guard.as_ref()
            && state.get_group(&default_group_id).await.is_some()
        {
            debug!("Default group already exists");
            return Ok(());
        }

        // Only create default group if we're the leader
        if consensus.is_leader().await {
            info!("Creating default group as the global consensus leader");

            // Create the default group with all cluster members
            let create_group_request = crate::consensus::global::GlobalRequest::CreateGroup {
                info: crate::foundation::GroupInfo {
                    id: default_group_id,
                    members: members.clone(),
                    created_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    metadata: Default::default(),
                },
            };

            consensus.submit_request(create_group_request).await?;
            info!("Successfully created default group through consensus");
        }

        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    /// Check if global consensus is ready (has initialized Raft layer and knows the leader)
    pub async fn is_ready(&self) -> ConsensusResult<Option<NodeId>> {
        // Check if consensus layer is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let _consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        // For single-node clusters, we can check metrics directly
        // For multi-node clusters, we'd need to check cluster exists
        let topology_manager = match self.topology_manager.as_ref() {
            Some(tm) => tm,
            None => return Ok(None),
        };

        // Check if we're in a single-node topology
        let peers = topology_manager.get_all_peers().await;
        if peers.is_empty() {
            // Single node - check our own Raft metrics
            let metrics_guard = self.raft_metrics_rx.read().await;
            if let Some(metrics_rx) = metrics_guard.as_ref() {
                let metrics = metrics_rx.borrow();
                // Return the current leader if one exists
                Ok(metrics.current_leader.clone())
            } else {
                // Metrics not available yet
                Ok(None)
            }
        } else {
            // Multi-node - use the existing check_cluster_exists logic
            match self.check_cluster_exists().await {
                Ok(Some((leader, _members))) => Ok(Some(leader)),
                Ok(None) => Ok(None),
                Err(_) => Ok(None), // Not ready yet
            }
        }
    }

    /// Get current global consensus members
    pub async fn get_members(&self) -> Vec<NodeId> {
        // Use the stored metrics receiver
        let metrics_guard = self.raft_metrics_rx.read().await;
        if let Some(metrics_rx) = metrics_guard.as_ref() {
            let metrics_data = metrics_rx.borrow();
            let membership = &metrics_data.membership_config;

            // Return all voter IDs
            membership.membership().voter_ids().collect()
        } else {
            // Consensus layer not initialized yet
            vec![]
        }
    }

    /// Add a node to global consensus
    pub async fn add_node_to_consensus(&self, node_id: NodeId) -> ConsensusResult<Vec<NodeId>> {
        info!("Adding node {} to global consensus", node_id);

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Global consensus not initialized",
                ));
            }
        };

        // Get node information from topology
        let node_info = if let Some(topology_mgr) = &self.topology_manager {
            topology_mgr.get_node(&node_id).await.ok_or_else(|| {
                Error::with_context(
                    ErrorKind::NotFound,
                    format!("Could not find node info for {node_id}"),
                )
            })?
        } else {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                "No topology manager available",
            ));
        };

        // Let the consensus layer handle adding the node
        consensus.add_node(node_id.clone(), node_info).await?;

        info!("Successfully added node {} to global consensus", node_id);

        // Return the updated membership
        let updated_metrics = consensus.metrics().borrow().clone();
        let updated_membership = &updated_metrics.membership_config;
        Ok(updated_membership.membership().voter_ids().collect())
    }

    /// Remove a node from global consensus
    pub async fn remove_node_from_consensus(
        &self,
        node_id: NodeId,
    ) -> ConsensusResult<Vec<NodeId>> {
        info!("Removing node {} from global consensus", node_id);

        // Ensure consensus is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                return Err(Error::with_context(
                    ErrorKind::InvalidState,
                    "Global consensus not initialized",
                ));
            }
        };

        // Let the consensus layer handle removing the node
        consensus.remove_node(node_id.clone()).await?;

        info!(
            "Successfully removed node {} from global consensus",
            node_id
        );

        // Return the updated membership
        let updated_metrics = consensus.metrics().borrow().clone();
        let updated_membership = &updated_metrics.membership_config;
        Ok(updated_membership.membership().voter_ids().collect())
    }

    /// Update global consensus membership (legacy method)
    pub async fn update_membership(
        &self,
        add_members: Vec<NodeId>,
        remove_members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        info!(
            "Updating global consensus membership - add: {:?}, remove: {:?}",
            add_members, remove_members
        );

        // Process removals first
        for node_id in remove_members {
            self.remove_node_from_consensus(node_id).await?;
        }

        // Then process additions
        for node_id in add_members {
            self.add_node_to_consensus(node_id).await?;
        }

        Ok(())
    }

    /// Check if a cluster already exists by querying peers
    async fn check_cluster_exists(&self) -> ConsensusResult<Option<(NodeId, Vec<NodeId>)>> {
        use super::messages::{
            CheckClusterExistsRequest, GlobalConsensusMessage, GlobalConsensusResponse,
        };

        let topology_manager = self.topology_manager.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Configuration, "No topology manager configured")
        })?;

        // Get all peers
        let peers = topology_manager.get_all_peers().await;
        if peers.is_empty() {
            tracing::info!("No peers found, no existing cluster");
            return Ok(None);
        }

        tracing::info!("Checking {} peers for existing cluster", peers.len());

        // Query each peer
        for peer in peers {
            tracing::debug!("Querying peer {} for cluster status", peer.node_id);

            let request = GlobalConsensusMessage::CheckClusterExists(CheckClusterExistsRequest {
                node_id: self.node_id.clone(),
            });

            match self
                .network_manager
                .service_request(peer.node_id.clone(), request, Duration::from_secs(5))
                .await
            {
                Ok(GlobalConsensusResponse::CheckClusterExists(response)) => {
                    if response.cluster_exists {
                        tracing::info!(
                            "Peer {} reports cluster exists with leader {:?} and {} members",
                            peer.node_id,
                            response.current_leader,
                            response.members.len()
                        );

                        // Return the leader and members
                        if let Some(leader) = response.current_leader {
                            return Ok(Some((leader, response.members)));
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to query peer {}: {}", peer.node_id, e);
                    // Continue to next peer
                }
                _ => {
                    tracing::warn!("Unexpected response from peer {}", peer.node_id);
                }
            }
        }

        tracing::info!("No existing cluster found after querying all peers");
        Ok(None)
    }

    /// Register command handlers for global consensus
    pub async fn register_command_handlers(self: Arc<Self>) {
        use crate::services::global_consensus::command_handlers::*;
        use crate::services::global_consensus::commands::*;

        // Register SubmitGlobalRequest handler
        let submit_handler = SubmitGlobalRequestHandler::new(self.clone());
        self.event_bus
            .handle_requests::<SubmitGlobalRequest, _>(submit_handler)
            .expect("Failed to register SubmitGlobalRequest handler");

        // Register InitializeGlobalConsensus handler
        let init_handler = InitializeGlobalConsensusHandler::new(self.clone());
        self.event_bus
            .handle_requests::<InitializeGlobalConsensus, _>(init_handler)
            .expect("Failed to register InitializeGlobalConsensus handler");

        // Register AddNodeToConsensus handler
        let add_node_handler = AddNodeToConsensusHandler::new(self.clone());
        self.event_bus
            .handle_requests::<AddNodeToConsensus, _>(add_node_handler)
            .expect("Failed to register AddNodeToConsensus handler");

        // Register RemoveNodeFromConsensus handler
        let remove_node_handler = RemoveNodeFromConsensusHandler::new(self.clone());
        self.event_bus
            .handle_requests::<RemoveNodeFromConsensus, _>(remove_node_handler)
            .expect("Failed to register RemoveNodeFromConsensus handler");

        // Register GetGlobalConsensusMembers handler
        let get_members_handler = GetGlobalConsensusMembersHandler::new(self.clone());
        self.event_bus
            .handle_requests::<GetGlobalConsensusMembers, _>(get_members_handler)
            .expect("Failed to register GetGlobalConsensusMembers handler");

        // Register UpdateGlobalMembership handler
        let update_membership_handler = UpdateGlobalMembershipHandler::new(self.clone());
        self.event_bus
            .handle_requests::<UpdateGlobalMembership, _>(update_membership_handler)
            .expect("Failed to register UpdateGlobalMembership handler");
    }
}
