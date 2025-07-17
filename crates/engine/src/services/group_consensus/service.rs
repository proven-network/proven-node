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

    /// Start event processing
    async fn start_event_processing(&self) {
        let event_service = match self.event_service.read().await.as_ref() {
            Some(service) => service.clone(),
            None => {
                tracing::warn!("Event service not set, skipping event processing");
                return;
            }
        };

        // Subscribe to consensus events
        let filter = EventFilter::ByType(vec![EventType::Consensus]);
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
