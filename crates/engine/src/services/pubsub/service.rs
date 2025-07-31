//! PubSub service implementation

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use tokio::sync::RwLock;
use tracing::info;

use crate::foundation::events::EventBus;
use crate::services::lifecycle::ComponentState;

use super::interest::InterestTracker;
use super::internal::{InterestManager, InterestPropagator, StreamManager};
use super::streaming_router::StreamingMessageRouter;

/// Configuration for PubSub service
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Default request timeout
    pub default_request_timeout: Duration,
    /// Interest update interval
    pub interest_update_interval: Duration,
    /// Maximum subscriptions per node
    pub max_subscriptions_per_node: usize,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            default_request_timeout: Duration::from_secs(30),
            interest_update_interval: Duration::from_secs(60),
            max_subscriptions_per_node: 1000,
        }
    }
}

/// PubSub service
pub struct PubSubService<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Configuration
    config: PubSubConfig,
    /// Local node ID
    node_id: NodeId,
    /// Interest tracker for distributed routing
    interest_tracker: InterestTracker,
    /// Local message router
    message_router: StreamingMessageRouter,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G, A>>,
    /// Event bus for publishing events
    event_bus: Arc<EventBus>,
    /// Service state
    state: Arc<RwLock<ComponentState>>,
    /// Known cluster members (maintained by membership events)
    cluster_members: Arc<RwLock<HashSet<NodeId>>>,
    /// Stream manager for publish and control streams
    stream_manager: StreamManager,
    /// Interest propagator
    interest_propagator: Arc<InterestPropagator>,
}

impl<T, G, A> PubSubService<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Create a new PubSub service
    pub async fn new(
        config: PubSubConfig,
        node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G, A>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        let stream_manager = StreamManager::new();
        let interest_tracker = InterestTracker::new();
        let interest_propagator = Arc::new(InterestPropagator::new(
            node_id.clone(),
            interest_tracker.clone(),
            stream_manager.get_control_streams(),
        ));

        Self {
            config,
            node_id: node_id.clone(),
            interest_tracker,
            message_router: StreamingMessageRouter::new(),
            network_manager: network_manager.clone(),
            event_bus,
            state: Arc::new(RwLock::new(ComponentState::NotInitialized)),
            cluster_members: Arc::new(RwLock::new(HashSet::new())),
            stream_manager,
            interest_propagator,
        }
    }
}

#[async_trait::async_trait]
impl<T, G, A> InterestManager for PubSubService<T, G, A>
where
    T: Transport + Send + Sync,
    G: TopologyAdaptor + Send + Sync,
    A: Attestor + Send + Sync,
{
    async fn add_node_interest(
        &self,
        node_id: NodeId,
        pattern: crate::foundation::types::SubjectPattern,
    ) {
        self.interest_tracker.add_interest(node_id, pattern).await;
    }

    async fn remove_node_interest(
        &self,
        node_id: NodeId,
        pattern: &crate::foundation::types::SubjectPattern,
    ) {
        self.interest_tracker
            .remove_interest(node_id, pattern)
            .await;
    }

    async fn remove_all_node_interests(&self, node_id: &NodeId) {
        self.interest_tracker.remove_node(node_id).await;
    }

    async fn update_peer_interests(
        &self,
        node_id: NodeId,
        patterns: Vec<crate::foundation::types::SubjectPattern>,
    ) {
        self.interest_tracker
            .update_interests(node_id, patterns)
            .await;
    }

    async fn route_message(&self, message: &crate::foundation::Message) -> usize {
        self.message_router.route(message).await
    }
}

impl<T, G, A> PubSubService<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Start the service
    pub async fn start(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting PubSub service for node {}", self.node_id);
        *self.state.write().await = ComponentState::Starting;

        // Register streaming service instead of request/response handler
        use super::streaming::PubSubStreamingService;

        let streaming_service =
            PubSubStreamingService::new(self.clone() as Arc<dyn InterestManager>);
        self.network_manager
            .register_streaming_service(streaming_service)
            .await
            .map_err(|e| format!("Failed to register streaming service: {e}"))?;

        // Setup event handlers
        use super::command_handlers::{HasRespondersHandler, PublishHandler, SubscribeHandler};
        use super::event_handlers::MembershipEventSubscriber;

        // Register command handlers for client service requests
        let publish_handler = PublishHandler::new(
            self.node_id.clone(),
            self.config.max_message_size,
            self.interest_tracker.clone(),
            self.message_router.clone(),
            self.stream_manager.clone(),
        );
        self.event_bus
            .handle_requests(publish_handler)
            .expect("Failed to register publish handler");

        // Register subscribe handler for streaming subscriptions
        let subscribe_handler = SubscribeHandler::new(
            self.node_id.clone(),
            self.config.max_subscriptions_per_node,
            self.interest_tracker.clone(),
            self.message_router.clone(),
            self.interest_propagator.clone(),
        );
        self.event_bus
            .handle_streams(subscribe_handler)
            .expect("Failed to register subscribe handler");

        // Register has_responders handler
        let has_responders_handler =
            HasRespondersHandler::new(self.interest_tracker.clone(), self.message_router.clone());
        self.event_bus
            .handle_requests(has_responders_handler)
            .expect("Failed to register has_responders handler");

        // Subscribe to membership events
        let membership_subscriber = MembershipEventSubscriber::new(
            self.node_id.clone(),
            self.interest_tracker.clone(),
            self.message_router.clone(),
            self.network_manager.clone(),
            self.cluster_members.clone(),
            self.interest_propagator.clone(),
        );
        let _membership_receiver = self.event_bus.subscribe(membership_subscriber);

        // Start stream maintenance task
        let stream_manager = self.stream_manager.clone();
        let node_id = self.node_id.clone();
        let cluster_members = self.cluster_members.clone();
        let network_manager = self.network_manager.clone();
        let interest_propagator = self.interest_propagator.clone();

        tokio::spawn(async move {
            stream_manager
                .maintain_streams(
                    &node_id,
                    cluster_members,
                    network_manager,
                    interest_propagator,
                )
                .await;
        });

        *self.state.write().await = ComponentState::Running;
        info!("PubSub service started");
        Ok(())
    }

    /// Stop the service
    pub async fn stop(self: Arc<Self>) -> Result<(), Box<dyn std::error::Error>> {
        info!("Stopping PubSub service");
        *self.state.write().await = ComponentState::ShuttingDown;

        // Clear all streams
        self.stream_manager.clear().await;

        // Unregister streaming service
        let _ = self
            .network_manager
            .unregister_streaming_service("pubsub")
            .await;

        // Unregister all event handlers to allow re-registration on restart
        use crate::services::pubsub::commands::{HasResponders, PublishMessage, Subscribe};
        let _ = self
            .event_bus
            .unregister_request_handler::<PublishMessage>();
        let _ = self.event_bus.unregister_stream_handler::<Subscribe>();
        let _ = self.event_bus.unregister_request_handler::<HasResponders>();

        // Clear all subscriptions
        self.message_router.clear().await;
        self.interest_tracker.clear().await;

        *self.state.write().await = ComponentState::Stopped;
        info!("PubSub service stopped");
        Ok(())
    }
}
