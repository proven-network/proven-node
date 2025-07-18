//! Group consensus service

use std::sync::Arc;
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::config::GroupConsensusConfig;
use crate::{
    consensus::group::GroupConsensusLayer,
    error::{ConsensusError, ConsensusResult, ErrorKind},
    foundation::types::ConsensusGroupId,
    services::event::{Event, EventEnvelope, EventFilter, EventPublisher, EventService, EventType},
};

/// Group consensus service
pub struct GroupConsensusService<T, G, L>
where
    T: Transport,
    G: TopologyAdaptor,
    L: LogStorage,
{
    /// Configuration
    config: GroupConsensusConfig,
    /// Node ID
    node_id: NodeId,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G>>,
    /// Storage
    storage: L,
    /// Group consensus layers
    groups: Arc<RwLock<std::collections::HashMap<ConsensusGroupId, Arc<GroupConsensusLayer<L>>>>>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
    /// Event service reference
    event_service: Arc<RwLock<Option<Arc<EventService>>>>,
}

impl<T, G, L> GroupConsensusService<T, G, L>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    L: LogStorage + 'static,
{
    /// Create new group consensus service
    pub fn new(
        config: GroupConsensusConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        storage: L,
    ) -> Self {
        Self {
            config,
            node_id,
            network_manager,
            storage,
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

        // Start listening for events
        self.start_event_processing().await;

        Ok(())
    }

    /// Stop the service
    pub async fn stop(&self) -> ConsensusResult<()> {
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
                self.storage.clone(),
            )
            .await?,
        );

        // Set event publisher if available
        if let Some(ref publisher) = self.event_publisher {
            layer.set_event_publisher(publisher.clone()).await;
        }

        // Store the layer
        self.groups.write().await.insert(group_id, layer.clone());

        // Initialize the group if we're one of the members
        if members.contains(&self.node_id) {
            // TODO: Properly initialize Raft cluster with members
            // For now, we'll skip Raft initialization for testing
            tracing::info!(
                "Would initialize group {} with members: {:?}",
                group_id,
                members
            );
        }

        // Emit event
        if let Some(ref publisher) = self.event_publisher {
            let _ = publisher
                .publish(
                    Event::GroupConsensusInitialized {
                        group_id,
                        node_id: self.node_id.clone(),
                        members,
                    },
                    "group_consensus".to_string(),
                )
                .await;
        }

        Ok(())
    }

    /// Register message handlers
    async fn register_message_handlers(&self) -> ConsensusResult<()> {
        // TODO: Register group consensus message handlers
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
        let stream_name = crate::stream::StreamName::new(stream_name);
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

    /// Start event processing
    async fn start_event_processing(&self) {
        let event_service = match self.event_service.read().await.as_ref() {
            Some(service) => service.clone(),
            None => {
                tracing::warn!("Event service not set, skipping event processing");
                return;
            }
        };

        // Subscribe to consensus, stream, and group events
        let filter = EventFilter::ByType(vec![
            EventType::Consensus,
            EventType::Stream,
            EventType::Group,
        ]);
        let mut subscriber = match event_service
            .subscribe("group-consensus-service".to_string(), filter)
            .await
        {
            Ok(sub) => sub,
            Err(e) => {
                tracing::error!("Failed to subscribe to events: {}", e);
                return;
            }
        };

        tracing::info!("GroupConsensusService: Successfully subscribed to consensus events");

        let service = self.clone();
        tokio::spawn(async move {
            tracing::info!("GroupConsensusService: Event processing task started");
            while let Some(envelope) = subscriber.recv().await {
                tracing::debug!(
                    "GroupConsensusService: Received event: {:?}",
                    envelope.event
                );
                if let Err(e) = service.handle_event(envelope).await {
                    tracing::error!("Error handling event: {}", e);
                }
            }
            tracing::info!("GroupConsensusService: Event processing task stopped");
        });
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
impl<T, G, L> Clone for GroupConsensusService<T, G, L>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    L: LogStorage + 'static,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            node_id: self.node_id.clone(),
            network_manager: self.network_manager.clone(),
            storage: self.storage.clone(),
            groups: self.groups.clone(),
            event_publisher: self.event_publisher.clone(),
            event_service: self.event_service.clone(),
        }
    }
}
