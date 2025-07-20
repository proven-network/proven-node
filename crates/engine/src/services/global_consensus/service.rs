//! Global consensus service

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use proven_network::NetworkManager;
use proven_storage::StorageNamespace;
use proven_storage::{ConsensusStorage, LogStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::{
    callbacks::GlobalConsensusCallbacksImpl,
    config::{GlobalConsensusConfig, ServiceState},
    topology_monitor::TopologyMonitor,
};
use crate::{
    consensus::global::{
        GlobalConsensusCallbacks, GlobalConsensusLayer, raft::GlobalRaftMessageHandler,
        types::GroupInfo,
    },
    error::{ConsensusResult, Error, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::stream::StreamName,
    services::{
        event::{Event, EventPublisher},
        group_consensus::GroupConsensusService,
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

/// Type alias for the topology monitor
type TopologyMonitorRef<G, S> = Arc<RwLock<Option<Arc<TopologyMonitor<G, S>>>>>;

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
    /// Task tracker for background tasks
    task_tracker: Arc<RwLock<Option<TaskTracker>>>,
    /// Cancellation token for graceful shutdown
    cancellation_token: Arc<RwLock<Option<CancellationToken>>>,
    /// Routing service for immediate consistency updates
    routing_service: Arc<RwLock<Option<Arc<crate::services::routing::RoutingService>>>>,
    /// Group consensus service for direct group creation
    group_consensus_service: GroupConsensusServiceRef<T, G, S>,
    /// Stream service for stream restoration
    stream_service: StreamServiceRef<S>,
    /// Topology monitor for membership updates
    topology_monitor: TopologyMonitorRef<G, S>,
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
            task_tracker: Arc::new(RwLock::new(None)),
            cancellation_token: Arc::new(RwLock::new(None)),
            routing_service: Arc::new(RwLock::new(None)),
            group_consensus_service: Arc::new(RwLock::new(None)),
            stream_service: Arc::new(RwLock::new(None)),
            topology_monitor: Arc::new(RwLock::new(None)),
            raft_metrics_rx: Arc::new(RwLock::new(None)),
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

    /// Set routing service for immediate consistency
    pub async fn set_routing_service(
        &self,
        routing_service: Arc<crate::services::routing::RoutingService>,
    ) {
        *self.routing_service.write().await = Some(routing_service);
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

        // Create new task tracker and cancellation token
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        {
            let mut tracker_guard = self.task_tracker.write().await;
            *tracker_guard = Some(task_tracker.clone());

            let mut token_guard = self.cancellation_token.write().await;
            *token_guard = Some(cancellation_token.clone());
        }

        // Start the default group checker task
        self.spawn_default_group_checker(&task_tracker, &cancellation_token);

        // Start topology monitor if we have a topology manager
        if let Some(ref topology_manager) = self.topology_manager {
            let monitor = Arc::new(TopologyMonitor::new(
                self.node_id.clone(),
                topology_manager.clone(),
                self.consensus_layer.clone(),
            ));

            monitor
                .clone()
                .start_monitoring(&task_tracker, &cancellation_token);

            let mut monitor_guard = self.topology_monitor.write().await;
            *monitor_guard = Some(monitor);
        }

        *state = ServiceState::Running;
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;
        drop(state);

        // Signal shutdown and wait for tasks
        let (task_tracker, cancellation_token) = {
            let tracker_guard = self.task_tracker.write().await;
            let token_guard = self.cancellation_token.write().await;

            (tracker_guard.clone(), token_guard.clone())
        };

        if let Some(token) = cancellation_token {
            tracing::debug!("Signaling cancellation to background tasks");
            token.cancel();
        }

        if let Some(tracker) = task_tracker {
            tracing::debug!("Waiting for background tasks to complete");
            tracker.close();

            match tokio::time::timeout(std::time::Duration::from_secs(5), tracker.wait()).await {
                Ok(()) => {
                    tracing::debug!("All background tasks completed successfully");
                }
                Err(_) => {
                    tracing::warn!("Background tasks did not complete within 5 seconds timeout");
                }
            }
        }

        // Clear the task tracker and cancellation token
        {
            let mut tracker_guard = self.task_tracker.write().await;
            *tracker_guard = None;

            let mut token_guard = self.cancellation_token.write().await;
            *token_guard = None;
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
            max_payload_entries: config.max_entries_per_append.get(),
            // TODO: Think about snapshotting
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        };

        let network_stats = Arc::new(RwLock::new(Default::default()));
        let network_factory = GlobalNetworkFactory::new(network_manager, network_stats);

        let layer = GlobalConsensusLayer::new(
            node_id.clone(),
            raft_config,
            network_factory,
            storage,
            callbacks,
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

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
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

    /// Check if we have persisted consensus state
    pub async fn has_persisted_state(&self) -> bool {
        // Check if we have a vote or committed index in storage
        let consensus_storage = self.storage_manager.consensus_storage();
        let namespace = StorageNamespace::new("global_logs");

        // Check for vote
        if let Ok(Some(_)) = consensus_storage.get_metadata(&namespace, "vote").await {
            return true;
        }

        // Check for committed
        if let Ok(Some(_)) = consensus_storage
            .get_metadata(&namespace, "committed")
            .await
        {
            return true;
        }

        // Check for log entries
        if let Ok(Some(_)) = consensus_storage.bounds(&namespace).await {
            return true;
        }

        false
    }

    /// Resume consensus from persisted state
    pub async fn resume_from_persisted_state(&self) -> ConsensusResult<()> {
        tracing::info!("Resuming global consensus from persisted state");

        // Check if already initialized
        if self.consensus_layer.read().await.is_some() {
            tracing::debug!("Global consensus layer already initialized");
            return Ok(());
        }

        // Always create consensus layer first so we can handle Raft messages
        let consensus_storage = self.storage_manager.consensus_storage();

        // Create callbacks with all wired services
        let routing_ref = self.routing_service.read().await.clone();
        let group_consensus_ref = self.group_consensus_service.read().await.clone();
        let stream_service_ref = self.stream_service.read().await.clone();

        let callbacks = Arc::new(GlobalConsensusCallbacksImpl::new(
            self.node_id.clone(),
            self.event_publisher.clone(),
            routing_ref,
            group_consensus_ref,
            stream_service_ref,
        ));

        let layer = Self::create_consensus_layer(
            &self.config,
            self.node_id.clone(),
            self.network_manager.clone(),
            consensus_storage,
            callbacks.clone(),
        )
        .await?;

        // Store the layer immediately so we can handle Raft messages
        {
            let mut consensus_guard = self.consensus_layer.write().await;
            *consensus_guard = Some(layer.clone());
        }

        // Store the metrics receiver
        {
            let metrics_rx = layer.metrics();
            let mut metrics_guard = self.raft_metrics_rx.write().await;
            *metrics_guard = Some(metrics_rx);
        }

        // Start event monitoring
        if let Some(ref publisher) = self.event_publisher {
            let metrics_rx = self.raft_metrics_rx.read().await.clone();
            if let Some(rx) = metrics_rx {
                Self::start_event_monitoring(rx, publisher.clone(), self.node_id.clone());
            }
        }

        // Emit event
        if let Some(ref publisher) = self.event_publisher {
            let _ = publisher
                .publish(
                    Event::GlobalConsensusInitialized {
                        node_id: self.node_id.clone(),
                        members: vec![self.node_id.clone()], // We don't know all members yet when resuming
                    },
                    "global_consensus".to_string(),
                )
                .await;
        }

        tracing::info!("Successfully resumed global consensus from persisted state");
        Ok(())
    }

    /// Start monitoring the consensus layer for events
    fn start_event_monitoring(
        mut metrics_rx: RaftMetricsReceiver,
        publisher: EventPublisher,
        node_id: NodeId,
    ) {
        // Monitor for leader changes
        let publisher_clone = publisher.clone();
        let node_id_clone = node_id.clone();

        tokio::spawn(async move {
            let mut current_leader: Option<NodeId> = None;
            let mut current_term: u64 = 0;

            loop {
                tokio::select! {
                    Ok(_) = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow().clone();

                        // Check for leader change
                        if metrics.current_leader != current_leader || metrics.current_term != current_term {
                            let old_leader = current_leader.clone();
                            current_leader = metrics.current_leader.clone();
                            current_term = metrics.current_term;

                            if let Some(new_leader) = &current_leader {
                                tracing::info!(
                                    "Global consensus leader changed: {:?} -> {} (term {})",
                                    old_leader, new_leader, current_term
                                );

                                // Publish event
                                let event = Event::GlobalLeaderChanged {
                                    old_leader,
                                    new_leader: new_leader.clone(),
                                    term: current_term,
                                };

                                let source = format!("global-consensus-{node_id_clone}");
                                if let Err(e) = publisher_clone.publish(event, source).await {
                                    tracing::warn!("Failed to publish GlobalLeaderChanged event: {}", e);
                                }
                            }
                        }
                    }
                    else => break,
                }
            }

            tracing::debug!("Leader monitoring task stopped");
        });
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
                .request_service(peer.node_id.clone(), request, Duration::from_secs(5))
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

    /// Initialize consensus directly from topology
    pub async fn initialize_from_topology(&self) -> ConsensusResult<()> {
        tracing::info!("Initializing global consensus from topology");

        // Check if already initialized
        if self.consensus_layer.read().await.is_some() {
            tracing::debug!("Global consensus layer already initialized");
            return Ok(());
        }

        // Register message handlers early so we can respond to CheckClusterExists
        Self::register_message_handlers_static(
            self.consensus_layer.clone(),
            self.network_manager.clone(),
        )
        .await?;

        // Get topology
        let topology_manager = self.topology_manager.as_ref().ok_or_else(|| {
            Error::with_context(ErrorKind::Configuration, "No topology manager configured")
        })?;

        let all_nodes = topology_manager
            .provider()
            .get_topology()
            .await
            .map_err(|e| {
                Error::with_context(ErrorKind::Network, format!("Failed to get topology: {e}"))
            })?;

        if all_nodes.is_empty() {
            return Err(Error::with_context(
                ErrorKind::Configuration,
                "No nodes found in topology",
            ));
        }

        tracing::info!("Found {} nodes in topology", all_nodes.len());

        // Create consensus layer
        let consensus_storage = self.storage_manager.consensus_storage();

        // Create callbacks
        let routing_ref = self.routing_service.read().await.clone();
        let group_consensus_ref = self.group_consensus_service.read().await.clone();
        let stream_service_ref = self.stream_service.read().await.clone();

        let callbacks = Arc::new(GlobalConsensusCallbacksImpl::new(
            self.node_id.clone(),
            self.event_publisher.clone(),
            routing_ref,
            group_consensus_ref,
            stream_service_ref,
        ));

        let layer = Self::create_consensus_layer(
            &self.config,
            self.node_id.clone(),
            self.network_manager.clone(),
            consensus_storage,
            callbacks.clone(),
        )
        .await?;

        // Store the metrics receiver
        {
            let metrics_rx = layer.metrics();
            let mut metrics_guard = self.raft_metrics_rx.write().await;
            *metrics_guard = Some(metrics_rx);
        }

        // Check if we have persisted state in storage
        let has_persisted_state = self.has_persisted_state().await;

        if !has_persisted_state {
            // Fresh cluster - check if a cluster already exists
            tracing::info!("No persisted state found, checking for existing cluster");

            // First, check if any peers already have a cluster
            let existing_cluster = self.check_cluster_exists().await?;

            if let Some((leader, members)) = existing_cluster {
                tracing::info!(
                    "Found existing cluster with leader {} and {} members, waiting to be added",
                    leader,
                    members.len()
                );
                // Don't initialize - the existing cluster will add us via topology monitor
            } else {
                tracing::info!("No existing cluster found, checking if we should initialize");

                // Sort nodes by ID to ensure deterministic selection
                let mut sorted_nodes = all_nodes.clone();
                sorted_nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

                // Only the first node (lowest ID) initializes the cluster
                if sorted_nodes
                    .first()
                    .map(|n| n.node_id == self.node_id)
                    .unwrap_or(false)
                {
                    tracing::info!(
                        "This node ({}) has the lowest ID, initializing new cluster",
                        self.node_id
                    );

                    // Start with just ourselves - topology monitor will add others
                    let mut raft_members = std::collections::BTreeMap::new();
                    raft_members.insert(
                        self.node_id.clone(),
                        all_nodes
                            .iter()
                            .find(|n| n.node_id == self.node_id)
                            .expect("Self not found in topology")
                            .clone(),
                    );

                    // Initialize cluster - this node will become the first leader
                    let handler: &dyn GlobalRaftMessageHandler = layer.as_ref();
                    handler.initialize_cluster(raft_members).await?;

                    tracing::info!("Raft cluster initialized by this node");
                } else {
                    tracing::info!(
                        "This node ({}) is not the initializer, waiting for cluster formation",
                        self.node_id
                    );
                    // Don't initialize - wait for the lowest ID node to initialize
                }
            }
        } else {
            // Has persisted state - Raft will resume and trigger election if needed
            tracing::info!("Resuming from persisted state");

            // Trigger state sync manually since we resumed from persisted state
            let state = layer.state();
            if let Err(e) = callbacks.on_state_synchronized(state).await {
                tracing::error!("State sync callback failed: {}", e);
            }
        }

        // Store the layer
        {
            let mut consensus_guard = self.consensus_layer.write().await;
            *consensus_guard = Some(layer.clone());
        }

        // Start event monitoring if we have an event publisher
        if let Some(ref publisher) = self.event_publisher {
            let metrics_rx = self.raft_metrics_rx.read().await.clone();
            if let Some(rx) = metrics_rx {
                Self::start_event_monitoring(rx, publisher.clone(), self.node_id.clone());
            }
        }

        // Emit event
        if let Some(ref publisher) = self.event_publisher {
            let _ = publisher
                .publish(
                    Event::GlobalConsensusInitialized {
                        node_id: self.node_id.clone(),
                        members: all_nodes.into_iter().map(|n| n.node_id).collect(),
                    },
                    "global_consensus".to_string(),
                )
                .await;
        }

        tracing::info!("Global consensus initialized successfully");
        Ok(())
    }

    /// Spawn the default group checker background task
    fn spawn_default_group_checker(
        &self,
        task_tracker: &TaskTracker,
        cancellation_token: &CancellationToken,
    ) {
        let consensus_layer = self.consensus_layer.clone();
        let node_id = self.node_id.clone();
        let topology_manager = self.topology_manager.clone();
        let token = cancellation_token.clone();

        task_tracker.spawn(async move {
            // Wait a bit for system to stabilize
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            // Periodically check and create default group if needed
            let mut check_interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        // Check if we have a consensus layer
                        let consensus_guard = consensus_layer.read().await;
                        if let Some(consensus) = consensus_guard.as_ref() {
                            // Check if default group exists first (regardless of leadership)
                            let state = consensus.state();
                            let default_group_id = crate::foundation::types::ConsensusGroupId::new(1);

                            if state.get_group(&default_group_id).await.is_some() {
                                tracing::debug!("Default group exists, exiting default group checker task");
                                return; // Group exists, exit the task
                            }

                            // Check if we're the leader
                            let metrics = consensus.metrics();
                            let is_leader = metrics.borrow().current_leader.as_ref() == Some(&node_id);

                            if is_leader {
                                tracing::info!("Default group not found, creating it");

                                // Get current cluster members from topology
                                let members = if let Some(tm) = &topology_manager {
                                    match tm.provider().get_topology().await {
                                        Ok(nodes) => nodes.into_iter().map(|n| n.node_id).collect(),
                                        Err(e) => {
                                            tracing::error!("Failed to get topology for default group: {}", e);
                                            continue;
                                        }
                                    }
                                } else {
                                    // If no topology manager, just use ourselves
                                    vec![node_id.clone()]
                                };

                                // Create the default group
                                let create_group_request = crate::consensus::global::GlobalRequest::CreateGroup {
                                    info: crate::consensus::global::types::GroupInfo {
                                        id: default_group_id,
                                        members,
                                        created_at: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_secs(),
                                        metadata: std::collections::HashMap::new(),
                                    },
                                };

                                match consensus.submit_request(create_group_request).await {
                                    Ok(_) => {
                                        tracing::info!("Successfully created default group through consensus");
                                        // Don't exit immediately - wait for next tick to confirm it exists
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to create default group: {}", e);
                                    }
                                }
                            } else {
                                tracing::debug!("Not the leader, waiting for default group to be created");
                            }
                        } else {
                            tracing::debug!("Consensus layer not initialized yet");
                        }
                    }
                    _ = token.cancelled() => {
                        tracing::debug!("Default group creation task cancelled");
                        return;
                    }
                }
            }
        });
    }
}
