//! Group consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::{ConsensusStorage, StorageAdaptor, StorageManager};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::config::GroupConsensusConfig;
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusError, ConsensusResult, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::event::{
        Event, EventEnvelope, EventFilter, EventHandler, EventPublisher, EventResult, EventService,
        EventType, EventingResult,
    },
};
use async_trait::async_trait;

/// Type alias for the consensus layers
type ConsensusLayers<S> = Arc<
    RwLock<
        std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<ConsensusStorage<S>>>>,
    >,
>;

/// Group consensus service
pub struct GroupConsensusService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Configuration
    config: GroupConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Group consensus layers
    groups: ConsensusLayers<S>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
    /// Event service reference
    event_service: Arc<RwLock<Option<Arc<EventService>>>>,
}

impl<T, G, S> GroupConsensusService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create new group consensus service
    pub fn new(
        config: GroupConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage_manager: Arc<StorageManager<S>>,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage_manager,
            groups: Arc::new(RwLock::new(std::collections::HashMap::new())),
            event_publisher: None,
            event_service: Arc::new(RwLock::new(None)),
        }
    }

    /// Set event publisher
    pub fn with_event_publisher(mut self, publisher: EventPublisher) -> Self {
        self.event_publisher = Some(publisher);
        self
    }

    /// Set event service
    pub async fn set_event_service(&self, event_service: Arc<EventService>) {
        *self.event_service.write().await = Some(event_service);
    }

    /// Start the service
    pub async fn start(&self) -> ConsensusResult<()> {
        // Register handlers
        self.register_message_handlers().await?;

        // Register event handlers for synchronous processing
        self.register_event_handlers().await;

        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
        // First, unregister the service handler from NetworkManager
        // Do this before shutting down Raft to avoid potential deadlocks
        use super::messages::GroupConsensusMessage;
        tracing::debug!("Unregistering group consensus service handler");
        if let Err(e) = self
            .network_manager
            .unregister_service::<GroupConsensusMessage>()
            .await
        {
            tracing::warn!("Failed to unregister group consensus service: {}", e);
        } else {
            tracing::debug!("Group consensus service handler unregistered");
        }

        // Shutdown all Raft instances in groups
        let mut groups = self.groups.write().await;
        for (group_id, layer) in groups.iter() {
            // Shutdown the Raft instance to release all resources with timeout
            match tokio::time::timeout(std::time::Duration::from_secs(5), layer.shutdown()).await {
                Ok(Ok(())) => {
                    tracing::debug!(
                        "Raft instance for group {:?} shut down successfully",
                        group_id
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!(
                        "Failed to shutdown Raft instance for group {:?}: {}",
                        group_id,
                        e
                    );
                }
                Err(_) => {
                    tracing::error!(
                        "Timeout while shutting down Raft instance for group {:?}",
                        group_id
                    );
                }
            }
        }

        // Clear all groups to release storage references
        groups.clear();
        drop(groups);

        Ok(())
    }

    /// Create a new group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        // Create the group consensus layer
        use super::adaptor::GroupNetworkFactory;
        use crate::consensus::group::GroupConsensusLayer;
        use openraft::Config;

        let raft_config = Config {
            cluster_name: format!("group-{:?}-{}", group_id, self.node_id),
            election_timeout_min: self.config.election_timeout_min.as_millis() as u64,
            election_timeout_max: self.config.election_timeout_max.as_millis() as u64,
            heartbeat_interval: self.config.heartbeat_interval.as_millis() as u64,
            max_payload_entries: self.config.max_entries_per_append,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(self.config.snapshot_interval),
            ..Default::default()
        };

        let network_stats = Arc::new(RwLock::new(Default::default()));
        let network_factory =
            GroupNetworkFactory::new(self.network_manager.clone(), group_id, network_stats);

        let layer = Arc::new(
            GroupConsensusLayer::new(
                self.node_id.clone(),
                group_id,
                raft_config,
                network_factory,
                self.storage_manager.consensus_storage(),
            )
            .await?,
        );

        // Store the layer
        self.groups.write().await.insert(group_id, layer.clone());

        // Initialize the group if we're one of the members
        if members.contains(&self.node_id) {
            // Initialize Raft cluster with members
            // For single-node groups, just use this node
            // For multi-node groups, we'd need to get full node info from topology
            let mut raft_members = std::collections::BTreeMap::new();
            for member_id in &members {
                // Create a minimal node info for Raft
                // In production, this should come from topology manager
                let node = proven_topology::Node::new(
                    "default-az".to_string(),
                    "http://127.0.0.1:0".to_string(), // Placeholder - not used for local groups
                    member_id.clone(),
                    "default-region".to_string(),
                    std::collections::HashSet::new(),
                );
                raft_members.insert(member_id.clone(), node);
            }

            tracing::info!(
                "Initializing group {} Raft cluster with {} members",
                group_id,
                raft_members.len()
            );

            // Initialize the Raft cluster
            if let Err(e) = layer.raft().initialize(raft_members).await {
                tracing::error!(
                    "Failed to initialize group {} Raft cluster: {}",
                    group_id,
                    e
                );
                return Err(ConsensusError::with_context(
                    ErrorKind::Consensus,
                    format!("Failed to initialize group Raft: {e}"),
                ));
            }

            tracing::info!("Successfully initialized group {} Raft cluster", group_id);
        }

        Ok(())
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
        use crate::consensus::group::raft::GroupRaftMessageHandler;

        let groups = self.groups.clone();

        // Register the service handler
        self.network_manager
            .register_service::<GroupConsensusMessage, _>(move |_sender, message| {
                let groups = groups.clone();
                Box::pin(async move {
                    match message {
                        GroupConsensusMessage::Vote { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_vote(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::Vote {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::AppendEntries { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_append_entries(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::AppendEntries {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::InstallSnapshot { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let handler: &dyn GroupRaftMessageHandler = layer.as_ref();
                            let resp = handler
                                .handle_install_snapshot(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::InstallSnapshot {
                                group_id,
                                response: resp,
                            })
                        }
                        GroupConsensusMessage::Consensus { group_id, request } => {
                            let groups_guard = groups.read().await;
                            let layer = groups_guard.get(&group_id).ok_or_else(|| {
                                proven_network::NetworkError::Other(format!(
                                    "Group {group_id} not found"
                                ))
                            })?;

                            let resp = layer
                                .submit_request(request)
                                .await
                                .map_err(|e| proven_network::NetworkError::Other(e.to_string()))?;
                            Ok(GroupConsensusServiceResponse::Consensus {
                                group_id,
                                response: resp,
                            })
                        }
                    }
                })
            })
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Network, e.to_string()))?;

        Ok(())
    }

    /// Check if service is healthy
    pub async fn is_healthy(&self) -> bool {
        true
    }

    /// Submit a request to a specific group
    pub async fn submit_to_group(
        &self,
        group_id: ConsensusGroupId,
        request: crate::consensus::group::GroupRequest,
    ) -> ConsensusResult<crate::consensus::group::GroupResponse> {
        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        layer.submit_request(request).await
    }

    /// Get stream state from a specific group
    pub async fn get_stream_state(
        &self,
        group_id: ConsensusGroupId,
        stream_name: &str,
    ) -> ConsensusResult<Option<crate::consensus::group::state::StreamState>> {
        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        let state = layer.state();
        let stream_name = crate::services::stream::StreamName::new(stream_name);
        Ok(state.get_stream(&stream_name).await)
    }

    /// Get all group IDs this node is a member of
    pub async fn get_node_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let groups = self.groups.read().await;
        // For now, return all groups that this node has joined
        // TODO: Properly track membership through Raft state
        Ok(groups.keys().cloned().collect())
    }

    /// Get group state information
    pub async fn get_group_state_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<super::types::GroupStateInfo> {
        use std::time::SystemTime;

        let groups = self.groups.read().await;
        let layer = groups.get(&group_id).ok_or_else(|| {
            ConsensusError::with_context(ErrorKind::NotFound, format!("Group {group_id} not found"))
        })?;

        // Get state from the layer
        let state = layer.state();

        // Get Raft metrics to find leader and term
        let metrics_rx = layer.metrics();
        let raft_metrics = metrics_rx.borrow();

        // Collect stream information
        let stream_names = state.list_streams().await;
        let mut stream_infos = Vec::new();
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;

        for stream_name in stream_names {
            if let Some(stream_state) = state.get_stream(&stream_name).await {
                let info = super::types::StreamInfo {
                    name: stream_name.to_string(),
                    message_count: stream_state.stats.message_count,
                    next_sequence: stream_state.next_sequence,
                    first_sequence: stream_state.first_sequence,
                    total_bytes: stream_state.stats.total_bytes,
                    last_update: stream_state.stats.last_update,
                };
                total_messages += stream_state.stats.message_count;
                total_bytes += stream_state.stats.total_bytes;
                stream_infos.push(info);
            }
        }

        // TODO: Get actual members from Raft membership config
        // For now, assume all nodes with this group are members
        let members = vec![self.node_id.clone()];
        let is_member = true;

        Ok(super::types::GroupStateInfo {
            group_id,
            members,
            leader: raft_metrics.current_leader.clone(),
            term: raft_metrics.current_term,
            is_member,
            streams: stream_infos,
            total_messages,
            total_bytes,
            created_at: SystemTime::now(), // TODO: Track actual creation time
            last_updated: SystemTime::now(),
        })
    }

    /// Register event handlers for synchronous processing
    async fn register_event_handlers(&self) {
        let event_service = match self.event_service.read().await.as_ref() {
            Some(service) => service.clone(),
            None => {
                tracing::warn!("Event service not set, skipping event handler registration");
                return;
            }
        };

        // Create handler
        let handler = Arc::new(GroupConsensusEventHandler::new(self.clone()));

        if let Err(e) = event_service
            .register_handler("group-consensus-handler".to_string(), handler)
            .await
        {
            tracing::error!("Failed to register group consensus event handler: {}", e);
            return;
        }

        // Add routes for the events we care about
        use crate::services::event::{EventRoute, EventRoutePattern};

        let consensus_route = EventRoute {
            pattern: EventRoutePattern::ByType(EventType::Consensus),
            handler_name: "group-consensus-handler".to_string(),
            priority: 100, // High priority for immediate consistency
        };

        if let Err(e) = event_service.add_route(consensus_route).await {
            tracing::error!("Failed to add consensus event route: {}", e);
        }

        let stream_route = EventRoute {
            pattern: EventRoutePattern::ByType(EventType::Stream),
            handler_name: "group-consensus-handler".to_string(),
            priority: 100,
        };

        if let Err(e) = event_service.add_route(stream_route).await {
            tracing::error!("Failed to add stream event route: {}", e);
        }

        let group_route = EventRoute {
            pattern: EventRoutePattern::ByType(EventType::Group),
            handler_name: "group-consensus-handler".to_string(),
            priority: 100,
        };

        if let Err(e) = event_service.add_route(group_route).await {
            tracing::error!("Failed to add group event route: {}", e);
        }

        tracing::info!("GroupConsensusService: Using synchronous event handlers");
    }

    /// Handle events
    async fn handle_event(&self, envelope: EventEnvelope) -> ConsensusResult<()> {
        match &envelope.event {
            Event::RequestDefaultGroupCreation { members } => {
                tracing::info!(
                    "Received request to create default group with {} members",
                    members.len()
                );

                // Create default group with ID 1
                let default_group_id = ConsensusGroupId::new(1);

                // Use the existing create_group method
                if let Err(e) = self.create_group(default_group_id, members.clone()).await {
                    tracing::error!("Failed to create default group: {}", e);
                } else {
                    tracing::info!("Successfully created default group");
                }
            }
            Event::StreamCreated {
                name,
                group_id,
                config: _,
            } => {
                tracing::info!(
                    "GroupConsensusService: Received StreamCreated event for {} in group {:?}",
                    name,
                    group_id
                );
                // Check if we have this group locally
                let groups = self.groups.read().await;
                if let Some(consensus) = groups.get(group_id) {
                    tracing::info!("Initializing stream {} in local group {:?}", name, group_id);
                    // Initialize the stream in the group
                    let request = crate::consensus::group::GroupRequest::Admin(
                        crate::consensus::group::types::AdminOperation::InitializeStream {
                            stream: name.clone(),
                        },
                    );
                    match consensus.submit_request(request).await {
                        Ok(_) => {
                            tracing::info!(
                                "Successfully initialized stream {} in group {:?}",
                                name,
                                group_id
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to initialize stream {} in group {:?}: {}",
                                name,
                                group_id,
                                e
                            );
                        }
                    }
                } else {
                    tracing::debug!(
                        "Group {:?} not found locally, ignoring StreamCreated event",
                        group_id
                    );
                }
            }
            Event::GroupCreated { group_id, members } => {
                tracing::info!(
                    "GroupConsensusService: Received GroupCreated event for group {:?} with members: {:?}",
                    group_id,
                    members
                );
                // Check if this node is a member of the group
                if members.contains(&self.node_id) {
                    tracing::info!("Creating local group {:?} as we are a member", group_id);
                    // Create the group locally
                    if let Err(e) = self.create_group(*group_id, members.clone()).await {
                        tracing::error!("Failed to create group {:?}: {}", group_id, e);
                    } else {
                        tracing::info!("Successfully created local group {:?}", group_id);
                    }
                } else {
                    tracing::debug!(
                        "Not a member of group {:?}, skipping local creation",
                        group_id
                    );
                }
            }
            _ => {
                // Ignore other events
            }
        }
        Ok(())
    }
}

// Implement Clone for the service
impl<T, G, S> Clone for GroupConsensusService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
            network_manager: self.network_manager.clone(),
            storage_manager: self.storage_manager.clone(),
            groups: self.groups.clone(),
            event_publisher: self.event_publisher.clone(),
            event_service: self.event_service.clone(),
        }
    }
}

/// Event handler for group consensus service
pub struct GroupConsensusEventHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    service: GroupConsensusService<T, G, S>,
}

impl<T, G, S> GroupConsensusEventHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    pub fn new(service: GroupConsensusService<T, G, S>) -> Self {
        Self { service }
    }
}

#[async_trait::async_trait]
impl<T, G, S> EventHandler for GroupConsensusEventHandler<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(&self, envelope: EventEnvelope) -> EventingResult<EventResult> {
        tracing::debug!(
            "GroupConsensusEventHandler: Processing event: {:?}",
            envelope.event
        );

        match self.service.handle_event(envelope).await {
            Ok(_) => Ok(EventResult::Success),
            Err(e) => Ok(EventResult::Failed(e.to_string())),
        }
    }

    fn name(&self) -> &str {
        "GroupConsensusEventHandler"
    }
}
