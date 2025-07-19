//! Global consensus service

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

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
    /// Task tracker for background tasks
    task_tracker: Arc<RwLock<Option<TaskTracker>>>,
    /// Cancellation token for graceful shutdown
    cancellation_token: Arc<RwLock<Option<CancellationToken>>>,
    /// Routing service for immediate consistency updates
    routing_service: Arc<RwLock<Option<Arc<crate::services::routing::RoutingService>>>>,
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

                        // Start event monitoring for the layer
                        if let Some(ref publisher) = event_publisher {
                            Self::start_event_monitoring(
                                layer.clone(),
                                publisher.clone(),
                                node_id.clone(),
                            );
                        }

                        // Store the layer clone for later use
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

        let layer =
            GlobalConsensusLayer::new(node_id.clone(), raft_config, network_factory, storage)
                .await?;

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
        let response = consensus.submit_request(request).await?;

        // Handle immediate consistency updates
        self.handle_consensus_response(&response).await;

        Ok(response)
    }

    /// Handle consensus response for immediate consistency
    async fn handle_consensus_response(&self, response: &crate::consensus::global::GlobalResponse) {
        use crate::consensus::global::types::GlobalResponse;

        match response {
            GlobalResponse::StreamCreated { name, group_id } => {
                // Update routing service immediately
                if let Some(routing) = self.routing_service.read().await.as_ref() {
                    if let Err(e) = routing
                        .update_stream_assignment(name.to_string(), *group_id)
                        .await
                    {
                        tracing::error!("Failed to update routing for new stream {}: {}", name, e);
                    } else {
                        tracing::info!(
                            "Updated routing for new stream {} -> group {:?}",
                            name,
                            group_id
                        );
                    }
                }

                // Publish event for eventual consistency
                if let Some(ref publisher) = self.event_publisher {
                    // Get stream config from state
                    if let Some(consensus) = self.consensus_layer.read().await.as_ref()
                        && let Some(stream_info) = consensus.state().get_stream(name).await
                    {
                        let event = Event::StreamCreated {
                            name: name.clone(),
                            config: stream_info.config.clone(),
                            group_id: *group_id,
                        };
                        let source = format!("global-consensus-{}", self.node_id);
                        if let Err(e) = publisher.publish(event, source).await {
                            tracing::warn!("Failed to publish StreamCreated event: {}", e);
                        }
                    }
                }
            }
            GlobalResponse::GroupCreated { id } => {
                // Get group info from state to get members
                if let Some(consensus) = self.consensus_layer.read().await.as_ref()
                    && let Some(group_info) = consensus.state().get_group(id).await
                {
                    // Update routing service immediately
                    if let Some(routing) = self.routing_service.read().await.as_ref() {
                        let group_route = crate::services::routing::GroupRoute {
                            group_id: *id,
                            members: group_info.members.clone(),
                            leader: None,    // Will be updated when leader is elected
                            stream_count: 0, // Will be updated as streams are added
                            health: crate::services::routing::GroupHealth::Healthy,
                            last_updated: std::time::SystemTime::now(),
                            location: if group_info.members.contains(&self.node_id) {
                                crate::services::routing::GroupLocation::Local
                            } else {
                                crate::services::routing::GroupLocation::Remote
                            },
                        };
                        if let Err(e) = routing.update_group_info(*id, group_route).await {
                            tracing::error!(
                                "Failed to update routing for new group {:?}: {}",
                                id,
                                e
                            );
                        } else {
                            tracing::info!("Updated routing for new group {:?}", id);
                        }
                    }

                    // Publish event for eventual consistency
                    if let Some(ref publisher) = self.event_publisher {
                        let event = Event::GroupCreated {
                            group_id: *id,
                            members: group_info.members.clone(),
                        };
                        let source = format!("global-consensus-{}", self.node_id);
                        if let Err(e) = publisher.publish(event, source).await {
                            tracing::warn!("Failed to publish GroupCreated event: {}", e);
                        }
                    }
                }
            }
            _ => {
                // Other responses don't need immediate handling
            }
        }
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        let state = self.state.read().await;
        *state == ServiceState::Running
    }

    /// Check and create default group if needed
    pub async fn ensure_default_group(&self, members: Vec<NodeId>) -> ConsensusResult<()> {
        // Check if consensus layer is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = match consensus_guard.as_ref() {
            Some(c) => c,
            None => {
                tracing::debug!("Global consensus not initialized, skipping default group check");
                return Ok(());
            }
        };

        // Check if we're the leader
        let metrics = consensus.metrics();
        let is_leader = metrics.borrow().current_leader.as_ref() == Some(&self.node_id);

        if !is_leader {
            tracing::debug!("Not the global consensus leader, skipping default group creation");
            return Ok(());
        }

        // Check if default group already exists
        let state = consensus.state();
        let default_group_id = crate::foundation::types::ConsensusGroupId::new(1);

        if state.get_group(&default_group_id).await.is_some() {
            tracing::debug!("Default group already exists");
            return Ok(());
        }

        tracing::info!("Creating default group as it doesn't exist");

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

        // Submit the request to create the default group
        match consensus.submit_request(create_group_request).await {
            Ok(_) => {
                tracing::info!("Successfully created default group through consensus");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to create default group: {}", e);
                Err(ConsensusError::with_context(
                    ErrorKind::Consensus,
                    format!("Failed to create default group: {e}"),
                ))
            }
        }
    }

    /// Start monitoring the consensus layer for events
    fn start_event_monitoring<L>(
        layer: Arc<GlobalConsensusLayer<L>>,
        publisher: EventPublisher,
        node_id: NodeId,
    ) where
        L: LogStorage + 'static,
    {
        // Monitor for leader changes
        let mut metrics_rx = layer.metrics();
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
