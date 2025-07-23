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

use tracing::{debug, error, info, warn};

use super::{
    callbacks::GlobalConsensusCallbacksImpl,
    config::{GlobalConsensusConfig, ServiceState},
};
use crate::services::global_consensus::subscribers::MembershipEventSubscriber;
use crate::{
    consensus::global::{
        GlobalConsensusCallbacks, GlobalConsensusLayer, raft::GlobalRaftMessageHandler,
    },
    error::{ConsensusResult, Error, ErrorKind},
    foundation::{GroupInfo, types::ConsensusGroupId},
    services::stream::StreamName,
    services::{
        event::bus::EventBus, global_consensus::GlobalConsensusEvent,
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
    /// Consensus layer (initialized after cluster formation)
    consensus_layer: ConsensusLayer<S>,
    /// Service state
    state: Arc<RwLock<ServiceState>>,
    /// Topology manager
    topology_manager: Option<Arc<proven_topology::TopologyManager<G>>>,
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Track if handlers have been registered
    handlers_registered: Arc<RwLock<bool>>,
    /// Task tracker for background tasks
    task_tracker: Arc<RwLock<Option<TaskTracker>>>,
    /// Cancellation token for graceful shutdown
    cancellation_token: Arc<RwLock<Option<CancellationToken>>>,
    /// Group consensus service for direct group creation
    group_consensus_service: GroupConsensusServiceRef<T, G, S>,
    /// Stream service for stream restoration
    stream_service: StreamServiceRef<S>,
    /// Membership service for cluster membership management
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
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            consensus_layer: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(ServiceState::NotInitialized)),
            topology_manager: None,
            event_bus,
            handlers_registered: Arc::new(RwLock::new(false)),
            task_tracker: Arc::new(RwLock::new(None)),
            cancellation_token: Arc::new(RwLock::new(None)),
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

        // Create new task tracker and cancellation token
        let task_tracker = TaskTracker::new();
        let cancellation_token = CancellationToken::new();

        {
            let mut tracker_guard = self.task_tracker.write().await;
            *tracker_guard = Some(task_tracker.clone());

            let mut token_guard = self.cancellation_token.write().await;
            *token_guard = Some(cancellation_token.clone());
        }

        // Create consensus layer early to handle messages
        if self.consensus_layer.read().await.is_none() {
            let consensus_storage = self.storage_manager.consensus_storage();
            let callbacks = Arc::new(GlobalConsensusCallbacksImpl::new(
                self.node_id.clone(),
                Some(self.event_bus.clone()),
            ));

            let layer = Self::create_consensus_layer(
                &self.config,
                self.node_id.clone(),
                self.network_manager.clone(),
                consensus_storage,
                callbacks.clone(),
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

            // Register message handlers
            Self::register_message_handlers_static(
                self.consensus_layer.clone(),
                self.network_manager.clone(),
            )
            .await?;
        }

        // Register membership event subscriber
        let membership_subscriber = MembershipEventSubscriber::new(
            self.node_id.clone(),
            self.consensus_layer.clone(),
            self.storage_manager.clone(),
            self.topology_manager.clone(),
        );

        self.event_bus.subscribe(membership_subscriber).await;

        // Start membership monitoring task
        self.spawn_membership_monitor(&task_tracker, &cancellation_token);

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

        let network_factory = GlobalNetworkFactory::new(network_manager);

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

        // Create callbacks with event bus only - all work is done by subscribers
        let callbacks = Arc::new(GlobalConsensusCallbacksImpl::new(
            self.node_id.clone(),
            Some(self.event_bus.clone()),
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
        let metrics_rx = self.raft_metrics_rx.read().await.clone();
        if let Some(rx) = metrics_rx {
            Self::start_event_monitoring(rx, self.event_bus.clone());
        }

        // Emit event with empty snapshot (actual state will be synchronized later)
        let snapshot = crate::services::global_consensus::events::GlobalStateSnapshot {
            groups: Vec::new(),
            streams: Vec::new(),
        };
        let event = GlobalConsensusEvent::StateSynchronized {
            snapshot: Box::new(snapshot),
        };
        self.event_bus.publish(event).await;

        tracing::info!("Successfully resumed global consensus from persisted state");
        Ok(())
    }

    /// Start monitoring the consensus layer for events
    fn start_event_monitoring(mut metrics_rx: RaftMetricsReceiver, event_bus: Arc<EventBus>) {
        // Monitor for leader changes
        let event_bus_clone = event_bus.clone();

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
                                let event = GlobalConsensusEvent::LeaderChanged {
                                    old_leader,
                                    new_leader: Some(new_leader.clone()),
                                    term: current_term,
                                };

                                event_bus_clone.publish(event).await;
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

    /// Spawn the membership monitor background task
    fn spawn_membership_monitor(
        &self,
        task_tracker: &TaskTracker,
        cancellation_token: &CancellationToken,
    ) {
        let consensus_layer = self.consensus_layer.clone();
        let membership_service = self.membership_service.clone();
        let node_id = self.node_id.clone();
        let _event_bus = self.event_bus.clone();
        let token = cancellation_token.clone();

        task_tracker.spawn(async move {
            tracing::info!("Starting membership monitor for global consensus");

            // Wait for membership service to be available
            loop {
                let membership_guard = membership_service.read().await;
                if membership_guard.is_some() {
                    drop(membership_guard);
                    break;
                }
                drop(membership_guard);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }

            // TODO: We need access to the EventService to subscribe to events
            // For now, we'll use a polling approach instead
            let mut check_interval = tokio::time::interval(std::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        // Periodically check membership and update Raft if needed
                        if let Err(e) = Self::update_raft_membership(
                            &consensus_layer,
                            &membership_service,
                            &node_id,
                        ).await {
                            tracing::error!("Failed to update Raft membership: {}", e);
                        }
                    }
                    _ = token.cancelled() => {
                        tracing::info!("Membership monitor shutting down");
                        return;
                    }
                }
            }
        });
    }

    /// Update Raft membership based on current membership view
    async fn update_raft_membership(
        consensus_layer: &ConsensusLayer<S>,
        membership_service: &MembershipServiceRef<T, G, S>,
        _node_id: &NodeId,
    ) -> ConsensusResult<()> {
        // Get consensus layer
        let consensus_guard = consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                return Ok(());
            }
        };

        // Only the leader should propose membership changes
        let is_leader = consensus.is_leader().await;
        if !is_leader {
            tracing::trace!("Not the leader, skipping membership update");
            return Ok(());
        }

        // Get membership service
        let membership_guard = membership_service.read().await;
        let membership = match membership_guard.as_ref() {
            Some(m) => m,
            None => {
                return Ok(());
            }
        };

        // Get online members from membership service
        let online_members = membership.get_online_members().await;
        let target_members: std::collections::BTreeSet<NodeId> =
            online_members.iter().map(|(id, _)| id.clone()).collect();

        // Get current Raft membership
        let raft_metrics = consensus.metrics();
        let current_membership = {
            let metrics = raft_metrics.borrow();
            metrics.membership_config.membership().clone()
        };

        let current_voters: std::collections::BTreeSet<NodeId> =
            current_membership.voter_ids().collect();
        let current_learners: std::collections::BTreeSet<NodeId> =
            current_membership.learner_ids().collect();
        let all_current: std::collections::BTreeSet<NodeId> =
            current_voters.union(&current_learners).cloned().collect();

        // Check if membership has changed
        if target_members == all_current {
            tracing::debug!("No membership changes needed");
            return Ok(());
        }

        tracing::info!(
            "Updating Raft membership. Current: {:?}, Target: {:?}",
            all_current,
            target_members
        );

        // Determine nodes to add and remove
        let nodes_to_add: Vec<NodeId> = target_members.difference(&all_current).cloned().collect();
        let nodes_to_remove: Vec<NodeId> =
            all_current.difference(&target_members).cloned().collect();

        // First, add new nodes as learners
        for node_id in &nodes_to_add {
            tracing::info!("Adding node {} as learner", node_id);

            // Find the node info from membership
            let node_info = online_members
                .iter()
                .find(|(id, _)| id == node_id)
                .map(|(_, info)| info.clone())
                .ok_or_else(|| {
                    Error::with_context(
                        ErrorKind::NotFound,
                        format!("Node {node_id} not found in online members"),
                    )
                })?;

            // Add as learner
            match consensus.add_learner(node_id.clone(), node_info).await {
                Ok(_) => tracing::info!("Successfully added {} as learner", node_id),
                Err(e) => {
                    tracing::warn!("Failed to add {} as learner: {}", node_id, e);
                    // Continue with other nodes
                }
            }
        }

        // Give learners time to catch up
        if !nodes_to_add.is_empty() {
            tracing::info!("Waiting for learners to catch up...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        // Now update membership to include new nodes as voters
        let mut new_voters = current_voters.clone();
        for node_id in nodes_to_add {
            new_voters.insert(node_id);
        }

        // Remove nodes that are no longer online
        for node_id in nodes_to_remove {
            new_voters.remove(&node_id);
        }

        // Propose membership change
        if new_voters != current_voters {
            tracing::info!("Proposing membership change: {:?}", new_voters);

            match consensus.change_membership(new_voters.clone(), false).await {
                Ok(_) => {
                    tracing::info!("Successfully updated membership");
                }
                Err(e) => {
                    tracing::error!("Failed to update membership: {}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
