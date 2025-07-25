//! PubSub service implementation

use base64::Engine as _;
use base64::engine::general_purpose;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use proven_topology::NodeId;

use crate::foundation::events::{EventBus, EventHandler};
use crate::foundation::types::ConsensusGroupId;
use crate::services::lifecycle::ComponentState;
use proven_network::NetworkManager;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::events::PubSubMessage;
use super::interest::InterestTracker;
use super::messages::{PubSubServiceMessage, PubSubServiceResponse};
use super::streaming_router::StreamingMessageRouter;
use super::types::*;
use crate::foundation::types::{Subject, SubjectPattern, subject_matches_pattern};

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
pub struct PubSubService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
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
    network: Arc<NetworkManager<T, G>>,
    /// Event bus for publishing events
    event_bus: Arc<EventBus>,
    /// Global consensus handle for querying stream mappings
    global_consensus_handle: Option<Arc<dyn GlobalConsensusHandle>>,
    /// Group consensus handle for stream writes
    group_consensus_handle: Option<Arc<dyn GroupConsensusHandle>>,
    /// Cached stream mappings from global consensus (subject pattern -> streams)
    stream_mappings_cache: Arc<RwLock<HashMap<String, Vec<StreamMapping>>>>,
    /// Last update time for cache
    cache_last_updated: Arc<RwLock<SystemTime>>,
    /// Service state
    state: Arc<RwLock<ComponentState>>,
    /// Statistics
    stats: Arc<RwLock<PubSubStats>>,
    /// Message channel for internal processing
    message_channel: (
        mpsc::Sender<PubSubNetworkMessage>,
        Option<mpsc::Receiver<PubSubNetworkMessage>>,
    ),
    /// Known cluster members (maintained by membership events)
    cluster_members: Arc<RwLock<HashSet<NodeId>>>,
    /// Type markers
    _transport: std::marker::PhantomData<T>,
    _governance: std::marker::PhantomData<G>,
}

impl<T, G> PubSubService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Get the node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the interest tracker
    pub fn interest_tracker(&self) -> &InterestTracker {
        &self.interest_tracker
    }

    /// Create a new PubSub service
    pub async fn new(
        config: PubSubConfig,
        node_id: NodeId,
        network: Arc<NetworkManager<T, G>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(10000);

        Self {
            config,
            node_id: node_id.clone(),
            interest_tracker: InterestTracker::new(),
            message_router: StreamingMessageRouter::new(),
            network: network.clone(),
            event_bus,
            global_consensus_handle: None,
            group_consensus_handle: None,
            stream_mappings_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_last_updated: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            state: Arc::new(RwLock::new(ComponentState::NotInitialized)),
            stats: Arc::new(RwLock::new(PubSubStats::default())),
            message_channel: (tx, Some(rx)),
            cluster_members: Arc::new(RwLock::new(HashSet::new())),
            _transport: std::marker::PhantomData,
            _governance: std::marker::PhantomData,
        }
    }

    /// Register network handlers (call after wrapping in Arc)
    pub async fn register_network_handlers(
        self: Arc<Self>,
        network: Arc<NetworkManager<T, G>>,
    ) -> PubSubResult<()> {
        let service = self.clone();

        network
            .register_service::<PubSubServiceMessage, _>(move |sender, message| {
                let service = service.clone();

                Box::pin(async move {
                    match message {
                        PubSubServiceMessage::Notify { messages } => {
                            // Handle batch of message notifications
                            for msg in messages {
                                debug!(
                                    "Processing message {} on subject {} from {}",
                                    msg.id, msg.subject, msg.source
                                );

                                // Convert to internal format and route
                                let network_msg = PubSubNetworkMessage {
                                    id: msg.id,
                                    subject: msg.subject,
                                    payload: msg.payload,
                                    headers: msg.headers,
                                    timestamp: msg.timestamp,
                                    source: msg.source,
                                    msg_type: PubSubMessageType::Publish,
                                };

                                service.message_router.route(&network_msg).await;
                            }
                            Ok(PubSubServiceResponse::Ack)
                        }
                        PubSubServiceMessage::RegisterInterest {
                            patterns,
                            timestamp: _,
                        } => {
                            // Update interest tracker with peer's interests
                            debug!(
                                "Received interest registration from {} with {} patterns",
                                sender,
                                patterns.len()
                            );
                            for pattern in &patterns {
                                debug!(
                                    "Adding interest from {} for pattern {}",
                                    sender,
                                    pattern.as_ref()
                                );
                                service
                                    .interest_tracker
                                    .add_interest(sender.clone(), pattern.clone())
                                    .await;
                            }
                            Ok(PubSubServiceResponse::Ack)
                        }
                        PubSubServiceMessage::UnregisterInterest {
                            patterns,
                            timestamp: _,
                        } => {
                            // Remove interests from tracker
                            for pattern in patterns {
                                service
                                    .interest_tracker
                                    .remove_interest(sender.clone(), &pattern)
                                    .await;
                            }
                            Ok(PubSubServiceResponse::Ack)
                        }
                    }
                })
            })
            .await
            .map_err(|e| PubSubError::Network(format!("Failed to register service: {e}")))?;

        Ok(())
    }

    /// Publish a message to a subject (Core NATS semantics: fire-and-forget)
    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: Vec<(String, String)>,
    ) -> PubSubResult<()> {
        // Validate subject
        let subject = Subject::new(&subject)?;

        // Check message size
        if payload.len() > self.config.max_message_size {
            return Err(PubSubError::MessageTooLarge(
                payload.len(),
                self.config.max_message_size,
            ));
        }

        let id = Uuid::new_v4();

        // Create message
        let message = PubSubNetworkMessage {
            id,
            subject,
            payload,
            headers,
            timestamp: std::time::SystemTime::now(),
            source: self.node_id.clone(),
            msg_type: PubSubMessageType::Publish,
        };

        // Update stats
        self.stats.write().await.messages_published += 1;

        // Route locally first
        let local_count = self.route_local_message(&message).await?;

        // Route to remote subscribers
        let remote_count = self.route_remote_message(&message).await?;

        // Update stats
        if local_count == 0 && remote_count == 0 {
            self.stats.write().await.messages_dropped += 1;
        }

        // Check if message should be persisted by streams
        self.handle_stream_subscriptions(&message).await?;

        Ok(())
    }

    /// Subscribe to a subject pattern
    pub async fn subscribe(
        &self,
        subject_pattern: String,
        queue_group: Option<String>,
    ) -> PubSubResult<(Subscription, flume::Receiver<PubSubMessage>)> {
        // Validate pattern
        let pattern = SubjectPattern::new(&subject_pattern)?;

        // Check subscription limit
        let current_subs = self
            .message_router
            .get_subscriptions()
            .await
            .into_iter()
            .filter(|s| s.node_id == self.node_id)
            .count();
        if current_subs >= self.config.max_subscriptions_per_node {
            return Err(PubSubError::MaxSubscriptionsExceeded(
                self.config.max_subscriptions_per_node,
            ));
        }

        let id = Uuid::new_v4().to_string();

        // Create subscription
        let subscription = Subscription {
            id: id.clone(),
            subject_pattern: pattern.clone(),
            node_id: self.node_id.clone(),
            queue_group: queue_group.clone(),
            created_at: std::time::SystemTime::now(),
        };

        // Create streaming channel
        let (tx, rx) = flume::unbounded();

        // Add to router with streaming channel
        self.message_router
            .add_streaming_subscription(id.clone(), subscription.clone(), tx)
            .await;

        // Update local interests
        self.interest_tracker
            .add_interest(self.node_id.clone(), pattern)
            .await;

        // Update stats
        self.stats.write().await.active_subscriptions += 1;

        // Interest updates are handled by update_local_interests below

        // Broadcast interest update
        self.update_local_interests().await?;

        Ok((subscription, rx))
    }

    /// Unsubscribe from a subscription
    pub async fn unsubscribe(&self, subscription_id: &str) -> PubSubResult<()> {
        if self
            .message_router
            .remove_subscription(subscription_id)
            .await
            .is_some()
        {
            // Update stats
            self.stats.write().await.active_subscriptions -= 1;

            // Interest updates are handled by update_local_interests below

            // Update interests
            self.update_local_interests().await?;

            Ok(())
        } else {
            Err(PubSubError::SubscriptionNotFound(
                subscription_id.to_string(),
            ))
        }
    }

    /// Route message to local subscribers and return count
    async fn route_local_message(&self, message: &PubSubNetworkMessage) -> PubSubResult<usize> {
        let count = self.message_router.route(message).await;
        Ok(count)
    }

    /// Route message to remote subscribers and return count
    async fn route_remote_message(&self, message: &PubSubNetworkMessage) -> PubSubResult<usize> {
        // Get interested nodes from interest tracker
        let interested_nodes = self
            .interest_tracker
            .find_interested_nodes(message.subject.as_str())
            .await;

        debug!(
            "Found {} interested nodes for subject {}",
            interested_nodes.len(),
            message.subject.as_str()
        );

        let mut count = 0;

        // Create message notification
        let notification = super::messages::MessageNotification {
            id: message.id,
            subject: message.subject.clone(),
            payload: message.payload.clone(),
            headers: message.headers.clone(),
            timestamp: message.timestamp,
            source: message.source.clone(),
        };

        // Batch messages for efficiency (we're sending one at a time for now)
        let service_msg = PubSubServiceMessage::Notify {
            messages: vec![notification],
        };

        // Send to interested remote nodes
        for node in interested_nodes {
            if node != self.node_id {
                // Use service_request with a short timeout for fire-and-forget behavior
                let _ = self
                    .network
                    .service_request(node, service_msg.clone(), Duration::from_millis(100))
                    .await;
                count += 1;
            }
        }

        if count > 0 {
            self.stats.write().await.messages_routed_remote += count as u64;
        }

        Ok(count)
    }

    /// Handle incoming message from network
    pub async fn handle_network_message(&self, message: PubSubNetworkMessage) -> PubSubResult<()> {
        // Update stats
        self.stats.write().await.messages_received += 1;

        match message.msg_type {
            PubSubMessageType::Publish => {
                // Route to local subscribers only (already routed remotely by sender)
                let count = self.route_local_message(&message).await?;
                if count == 0 {
                    self.stats.write().await.messages_dropped += 1;
                }
            }
            PubSubMessageType::Control => {
                // Control messages are handled at the service message level
                // This should not be reached in normal operation
            }
        }

        Ok(())
    }

    /// Update local interests and broadcast to peers
    async fn update_local_interests(&self) -> PubSubResult<()> {
        let subscriptions = self
            .message_router
            .get_node_subscriptions(&self.node_id)
            .await;
        let patterns: Vec<SubjectPattern> = subscriptions
            .into_iter()
            .map(|sub| sub.subject_pattern)
            .collect();

        // Update local tracker
        self.interest_tracker
            .update_interests(self.node_id.clone(), patterns.clone())
            .await;

        // Send to all cluster members (not just nodes with interests)
        let cluster_members = self.cluster_members.read().await.clone();
        debug!(
            "Broadcasting interests to {} cluster members (patterns: {:?})",
            cluster_members.len(),
            patterns.len()
        );

        // Broadcast to peers
        let msg = PubSubServiceMessage::RegisterInterest {
            patterns,
            timestamp: std::time::SystemTime::now(),
        };
        for node in cluster_members {
            if node != self.node_id {
                debug!("Sending interests to node {}", node);
                let _ = self
                    .network
                    .service_request(node, msg.clone(), Duration::from_millis(100))
                    .await;
            }
        }

        Ok(())
    }

    /// Get service statistics
    pub async fn get_stats(&self) -> PubSubStats {
        self.stats.read().await.clone()
    }

    /// Get the message router (for subscribers)
    pub fn message_router(&self) -> &StreamingMessageRouter {
        &self.message_router
    }

    /// Setup event handler for PubSub events (call after wrapping in Arc)
    pub fn setup_event_handler(self: Arc<Self>) {
        use super::command_handlers::{
            PublishMessageHandler, SubscribeHandler, UnsubscribeHandler,
        };
        use super::event_handlers::{GlobalConsensusEventSubscriber, MembershipEventSubscriber};
        use super::streaming_handlers::SubscribeStreamHandler;

        // Register command handlers for client service requests
        let publish_handler = PublishMessageHandler::new(self.clone());
        self.event_bus
            .handle_requests(publish_handler)
            .expect("Failed to register publish handler");

        let subscribe_handler = SubscribeHandler::new(self.clone());
        self.event_bus
            .handle_requests(subscribe_handler)
            .expect("Failed to register subscribe handler");

        let unsubscribe_handler = UnsubscribeHandler::new(self.clone());
        self.event_bus
            .handle_requests(unsubscribe_handler)
            .expect("Failed to register unsubscribe handler");

        // Register streaming handler for efficient subscriptions
        let streaming_handler = SubscribeStreamHandler::new(self.clone());
        self.event_bus
            .handle_streams(streaming_handler)
            .expect("Failed to register streaming subscription handler");

        // Subscribe to membership events
        let membership_subscriber =
            MembershipEventSubscriber::new(self.clone(), self.event_bus.clone());
        let _membership_receiver = self.event_bus.subscribe(membership_subscriber);

        // Subscribe to global consensus events
        let global_consensus_subscriber =
            GlobalConsensusEventSubscriber::new(self.clone(), self.event_bus.clone());
        let _global_consensus_receiver = self.event_bus.subscribe(global_consensus_subscriber);
    }

    /// Refresh stream mappings from global consensus
    pub async fn refresh_stream_mappings(&self) -> PubSubResult<()> {
        if let Some(global_consensus) = &self.global_consensus_handle {
            match global_consensus.get_stream_mappings().await {
                Ok(mappings) => {
                    let mut cache = self.stream_mappings_cache.write().await;
                    cache.clear();

                    // Group mappings by subject pattern
                    for mapping in mappings {
                        cache
                            .entry(mapping.subject_pattern.clone())
                            .or_insert_with(Vec::new)
                            .push(mapping);
                    }

                    *self.cache_last_updated.write().await = SystemTime::now();
                    debug!(
                        "Refreshed {} stream mappings from global consensus",
                        cache.len()
                    );
                }
                Err(e) => {
                    warn!("Failed to refresh stream mappings: {}", e);
                    return Err(PubSubError::Internal(format!(
                        "Failed to refresh mappings: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Handle stream subscriptions for a message
    async fn handle_stream_subscriptions(
        &self,
        message: &PubSubNetworkMessage,
    ) -> PubSubResult<()> {
        // Check if cache needs refresh (e.g., older than 30 seconds)
        let last_updated = *self.cache_last_updated.read().await;
        if SystemTime::now()
            .duration_since(last_updated)
            .unwrap_or(Duration::from_secs(60))
            > Duration::from_secs(30)
        {
            self.refresh_stream_mappings().await?;
        }

        let mappings = self.stream_mappings_cache.read().await;

        // Find matching stream mappings
        for (pattern, stream_mappings) in mappings.iter() {
            if subject_matches_pattern(message.subject.as_str(), pattern) {
                for mapping in stream_mappings {
                    if mapping.auto_publish {
                        self.publish_to_stream_via_consensus(mapping, message)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Publish message to stream through group consensus
    async fn publish_to_stream_via_consensus(
        &self,
        mapping: &StreamMapping,
        message: &PubSubNetworkMessage,
    ) -> PubSubResult<()> {
        if let Some(consensus_handle) = &self.group_consensus_handle {
            // Create stream entry from PubSub message
            let stream_entry = serde_json::json!({
                "pubsub_message_id": message.id,
                "subject": message.subject,
                "payload": general_purpose::STANDARD.encode(&message.payload),
                "headers": message.headers,
                "source": message.source,
                "timestamp": message.timestamp,
            });

            let entry_bytes = serde_json::to_vec(&stream_entry)
                .map_err(|e| PubSubError::Internal(e.to_string()))?;

            // Submit to group consensus for writing to stream
            consensus_handle
                .publish_to_stream(
                    mapping.group_id,
                    mapping.stream_id.clone(),
                    Bytes::from(entry_bytes),
                )
                .await
                .map_err(|e| PubSubError::Internal(format!("Consensus error: {e}")))?;

            info!(
                "Published message {} to stream {} via group consensus",
                message.id, mapping.stream_id
            );

            self.stats.write().await.messages_persisted += 1;
        } else {
            warn!("No group consensus handle available for stream publishing");
        }

        Ok(())
    }

    /// Set the global consensus handle
    pub fn set_global_consensus_handle(&mut self, handle: Arc<dyn GlobalConsensusHandle>) {
        self.global_consensus_handle = Some(handle);
    }

    /// Set the group consensus handle
    pub fn set_group_consensus_handle(&mut self, handle: Arc<dyn GroupConsensusHandle>) {
        self.group_consensus_handle = Some(handle);
    }

    /// Broadcast current interests to specific nodes
    pub async fn broadcast_interests_to_nodes(&self, nodes: &[NodeId]) -> PubSubResult<()> {
        let subscriptions = self
            .message_router
            .get_node_subscriptions(&self.node_id)
            .await;

        if subscriptions.is_empty() {
            return Ok(());
        }

        let patterns: Vec<SubjectPattern> = subscriptions
            .into_iter()
            .map(|sub| sub.subject_pattern)
            .collect();

        let msg = PubSubServiceMessage::RegisterInterest {
            patterns,
            timestamp: std::time::SystemTime::now(),
        };

        // Send to specified nodes
        for node in nodes {
            if node != &self.node_id
                && let Err(e) = self
                    .network
                    .service_request(node.clone(), msg.clone(), Duration::from_millis(100))
                    .await
            {
                debug!("Failed to send interests to node {}: {}", node, e);
            }
        }

        Ok(())
    }

    /// Remove a node's interests from the tracker
    pub async fn remove_node_interests(&self, node_id: &NodeId) {
        self.interest_tracker.remove_node(node_id).await;
        self.cluster_members.write().await.remove(node_id);
    }

    /// Update cluster members list (called by membership subscriber)
    pub async fn update_cluster_members(&self, members: &[NodeId]) {
        let mut cluster_members = self.cluster_members.write().await;
        cluster_members.clear();
        cluster_members.extend(members.iter().cloned());
    }

    /// Add a cluster member
    pub async fn add_cluster_member(&self, node_id: &NodeId) {
        self.cluster_members.write().await.insert(node_id.clone());
    }
}

impl<T, G> PubSubService<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Start the service
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting PubSub service");
        *self.state.write().await = ComponentState::Starting;

        // Start message processing loop
        if let Some(mut receiver) = self.message_channel.1.take() {
            let _message_router = self.message_router.clone();
            let _stats = self.stats.clone();

            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    // Process message
                    debug!("Processing PubSub message: {}", message.id);
                    // Message processing is handled by handle_network_message
                }
            });
        }

        *self.state.write().await = ComponentState::Running;
        info!("PubSub service started");
        Ok(())
    }

    /// Stop the service
    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Stopping PubSub service");
        *self.state.write().await = ComponentState::ShuttingDown;

        // Clear all subscriptions
        self.message_router.clear().await;
        self.interest_tracker.clear().await;

        *self.state.write().await = ComponentState::Stopped;
        info!("PubSub service stopped");
        Ok(())
    }

    /// Health check
    pub async fn health_check(&self) -> Result<(), Box<dyn std::error::Error>> {
        match *self.state.read().await {
            ComponentState::Running => Ok(()),
            state => Err(format!("PubSub service not running: {state:?}").into()),
        }
    }
}
