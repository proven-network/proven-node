//! PubSub service implementation

use base64::Engine as _;
use base64::engine::general_purpose;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use proven_topology::NodeId;

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::{Event, EventPublisher};
use crate::services::lifecycle::ComponentState;
use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_transport::Transport;

use super::interest::InterestTracker;
use super::messages::{InterestUpdateMessage, PubSubMessageType, PubSubNetworkMessage};
use super::router::MessageRouter;
use super::subject::{Subject, SubjectPattern, subject_matches_pattern};
use super::types::*;

/// Configuration for PubSub service
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Default request timeout
    pub default_request_timeout: Duration,
    /// Interest update interval
    pub interest_update_interval: Duration,
    /// Enable persistence for matching patterns
    pub persistence_patterns: Vec<String>,
    /// Maximum subscriptions per node
    pub max_subscriptions_per_node: usize,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            default_request_timeout: Duration::from_secs(30),
            interest_update_interval: Duration::from_secs(60),
            persistence_patterns: vec!["stream.>".to_string(), "consensus.>".to_string()],
            max_subscriptions_per_node: 1000,
        }
    }
}

/// PubSub service
pub struct PubSubService<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Configuration
    config: PubSubConfig,
    /// Local node ID
    node_id: NodeId,
    /// Interest tracker for distributed routing
    interest_tracker: InterestTracker,
    /// Local message router
    message_router: MessageRouter,
    /// Pending requests waiting for responses
    pending_requests: Arc<RwLock<HashMap<Uuid, oneshot::Sender<PubSubResponse>>>>,
    /// Network manager
    network: Option<Arc<NetworkManager<T, G>>>,
    /// Event publisher for persistence bridge
    event_publisher: Option<EventPublisher>,
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
    /// Type markers
    _transport: std::marker::PhantomData<T>,
    _governance: std::marker::PhantomData<G>,
}

impl<T, G> PubSubService<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new PubSub service
    pub fn new(config: PubSubConfig, node_id: NodeId) -> Self {
        let (tx, rx) = mpsc::channel(10000);
        Self {
            config,
            node_id,
            interest_tracker: InterestTracker::new(),
            message_router: MessageRouter::new(),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            network: None,
            event_publisher: None,
            global_consensus_handle: None,
            group_consensus_handle: None,
            stream_mappings_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_last_updated: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
            state: Arc::new(RwLock::new(ComponentState::NotInitialized)),
            stats: Arc::new(RwLock::new(PubSubStats::default())),
            message_channel: (tx, Some(rx)),
            _transport: std::marker::PhantomData,
            _governance: std::marker::PhantomData,
        }
    }

    /// Set the network manager
    pub fn set_network(&mut self, network: Arc<NetworkManager<T, G>>) {
        self.network = Some(network);
    }

    /// Set the event publisher
    pub fn set_event_publisher(&mut self, publisher: EventPublisher) {
        self.event_publisher = Some(publisher);
    }

    /// Publish a message
    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: Vec<(String, String)>,
    ) -> PubSubResult<Uuid> {
        // Validate subject
        let subject = Subject::new(&subject)?;

        // Check message size
        if payload.len() > self.config.max_message_size {
            return Err(PubSubError::MessageTooLarge(
                payload.len(),
                self.config.max_message_size,
            ));
        }

        // Create message
        let message = PubSubNetworkMessage {
            id: Uuid::new_v4(),
            subject: subject.to_string(),
            payload,
            headers,
            timestamp: std::time::SystemTime::now(),
            source: self.node_id.clone(),
            msg_type: PubSubMessageType::Publish,
            reply_to: None,
            correlation_id: None,
        };

        // Route locally
        self.route_local_message(&message).await?;

        // Route to interested remote nodes
        self.route_remote_message(&message).await?;

        // Update stats
        self.stats.write().await.messages_published += 1;

        Ok(message.id)
    }

    /// Send a request and wait for response
    pub async fn request(
        &self,
        subject: String,
        payload: Bytes,
        headers: Vec<(String, String)>,
        timeout_duration: Option<Duration>,
    ) -> PubSubResult<PubSubResponse> {
        // Validate subject
        let subject = Subject::new(&subject)?;

        // Create reply subject
        let reply_subject = format!("_INBOX.{}", Uuid::new_v4());

        // Create request message
        let message = PubSubNetworkMessage {
            id: Uuid::new_v4(),
            subject: subject.to_string(),
            payload,
            headers,
            timestamp: std::time::SystemTime::now(),
            source: self.node_id.clone(),
            msg_type: PubSubMessageType::Request,
            reply_to: Some(reply_subject.clone()),
            correlation_id: None,
        };

        // Create response channel
        let (tx, rx) = oneshot::channel();
        self.pending_requests.write().await.insert(message.id, tx);

        // Subscribe to reply subject temporarily
        let reply_sub = Subscription {
            id: format!("reply_{}", message.id),
            subject_pattern: reply_subject,
            node_id: self.node_id.clone(),
            persist: false,
            queue_group: None,
            delivery_mode: DeliveryMode::BestEffort,
            created_at: std::time::SystemTime::now(),
        };
        self.message_router
            .add_subscription(reply_sub.clone())
            .await;

        // Route the request
        self.route_local_message(&message).await?;
        self.route_remote_message(&message).await?;

        // Wait for response with timeout
        let timeout_duration = timeout_duration.unwrap_or(self.config.default_request_timeout);
        let result = match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(PubSubError::Internal("Response channel closed".to_string())),
            Err(_) => {
                self.stats.write().await.request_timeouts += 1;
                Err(PubSubError::RequestTimeout)
            }
        };

        // Clean up
        self.pending_requests.write().await.remove(&message.id);
        self.message_router.remove_subscription(&reply_sub.id).await;

        result
    }

    /// Subscribe to a subject pattern
    pub async fn subscribe(
        &self,
        subject_pattern: String,
        queue_group: Option<String>,
        delivery_mode: DeliveryMode,
        persist: bool,
    ) -> PubSubResult<String> {
        // Validate pattern
        let pattern = SubjectPattern::new(&subject_pattern)?;

        // Check subscription limit
        let current_subs = self
            .message_router
            .get_node_subscriptions(&self.node_id)
            .await;
        if current_subs.len() >= self.config.max_subscriptions_per_node {
            return Err(PubSubError::Internal(format!(
                "Subscription limit reached: {}",
                self.config.max_subscriptions_per_node
            )));
        }

        // Create subscription
        let subscription = Subscription {
            id: Uuid::new_v4().to_string(),
            subject_pattern: pattern.to_string(),
            node_id: self.node_id.clone(),
            persist,
            queue_group,
            delivery_mode,
            created_at: std::time::SystemTime::now(),
        };

        // Add to router
        let sub_id = self
            .message_router
            .add_subscription(subscription.clone())
            .await;

        // Update local interests
        self.update_local_interests().await?;

        // Update stats
        self.stats.write().await.active_subscriptions += 1;

        Ok(sub_id)
    }

    /// Unsubscribe from a subscription
    pub async fn unsubscribe(&self, subscription_id: &str) -> PubSubResult<()> {
        if self
            .message_router
            .remove_subscription(subscription_id)
            .await
            .is_some()
        {
            self.update_local_interests().await?;
            self.stats.write().await.active_subscriptions -= 1;
            Ok(())
        } else {
            Err(PubSubError::SubscriptionNotFound(
                subscription_id.to_string(),
            ))
        }
    }

    /// Route message to local subscribers
    async fn route_local_message(&self, message: &PubSubNetworkMessage) -> PubSubResult<()> {
        let subscriptions = self.message_router.route_message(&message.subject).await;

        for subscription in subscriptions {
            if subscription.node_id == self.node_id {
                // Handle local delivery
                debug!(
                    "Delivering message {} to local subscription {}",
                    message.id, subscription.id
                );

                // Check if we need to persist
                if subscription.persist || self.should_persist(&message.subject) {
                    self.publish_persistence_event(message).await?;
                }

                // Check for stream subscriptions
                self.handle_stream_subscriptions(message).await?;
            }
        }

        Ok(())
    }

    /// Route message to remote interested nodes
    async fn route_remote_message(&self, message: &PubSubNetworkMessage) -> PubSubResult<()> {
        let interested_nodes = self
            .interest_tracker
            .find_interested_nodes(&message.subject)
            .await;

        for node_id in interested_nodes {
            if node_id != self.node_id {
                debug!("Routing message {} to remote node {}", message.id, node_id);

                // Send via network
                if let Some(_network) = &self.network {
                    // In a real implementation, we'd send via the network service
                    // For now, this is a placeholder
                    debug!("Would send PubSub message to {}", node_id);
                }
            }
        }

        Ok(())
    }

    /// Handle incoming message from network
    pub async fn handle_network_message(&self, message: PubSubNetworkMessage) -> PubSubResult<()> {
        // Update stats
        self.stats.write().await.messages_received += 1;

        match message.msg_type {
            PubSubMessageType::Publish => {
                // Route to local subscribers only (already routed remotely by sender)
                self.route_local_message(&message).await?;
            }
            PubSubMessageType::Request => {
                // Handle request - route and potentially respond
                self.route_local_message(&message).await?;
            }
            PubSubMessageType::Response => {
                // Handle response to a pending request
                if let Some(correlation_id) = message.correlation_id
                    && let Some(sender) =
                        self.pending_requests.write().await.remove(&correlation_id)
                {
                    let response = PubSubResponse::Response {
                        payload: message.payload,
                        headers: message.headers,
                    };
                    let _ = sender.send(response);
                }
            }
            PubSubMessageType::Control => {
                // Handle control messages (interest updates, etc.)
                self.handle_control_message(message).await?;
            }
        }

        Ok(())
    }

    /// Handle control messages
    async fn handle_control_message(&self, message: PubSubNetworkMessage) -> PubSubResult<()> {
        // For now, assume control messages are interest updates
        if let Ok(update) = serde_json::from_slice::<InterestUpdateMessage>(&message.payload) {
            self.interest_tracker
                .update_interests(update.node_id, update.interests)
                .await;
        }
        Ok(())
    }

    /// Update local interests and broadcast to peers
    async fn update_local_interests(&self) -> PubSubResult<()> {
        let subscriptions = self
            .message_router
            .get_node_subscriptions(&self.node_id)
            .await;
        let patterns: Vec<String> = subscriptions
            .into_iter()
            .map(|sub| sub.subject_pattern)
            .collect();

        // Update local tracker
        self.interest_tracker
            .update_interests(self.node_id.clone(), patterns.clone())
            .await;

        // Broadcast to peers
        let update = InterestUpdateMessage {
            node_id: self.node_id.clone(),
            interests: patterns,
            timestamp: std::time::SystemTime::now(),
        };

        let payload =
            serde_json::to_vec(&update).map_err(|e| PubSubError::Internal(e.to_string()))?;

        let _control_msg = PubSubNetworkMessage {
            id: Uuid::new_v4(),
            subject: "_CONTROL.interest.update".to_string(),
            payload: Bytes::from(payload),
            headers: vec![],
            timestamp: std::time::SystemTime::now(),
            source: self.node_id.clone(),
            msg_type: PubSubMessageType::Control,
            reply_to: None,
            correlation_id: None,
        };

        // Send to all known nodes
        if let Some(_network) = &self.network {
            // In a real implementation, we'd broadcast the control message
            // For now, this is a placeholder
            debug!("Would broadcast interest update");
        }

        Ok(())
    }

    /// Check if a subject should be persisted
    fn should_persist(&self, subject: &str) -> bool {
        self.config
            .persistence_patterns
            .iter()
            .any(|pattern| subject_matches_pattern(subject, pattern))
    }

    /// Publish event for persistence
    async fn publish_persistence_event(&self, message: &PubSubNetworkMessage) -> PubSubResult<()> {
        if let Some(publisher) = &self.event_publisher {
            let event = Event::Custom {
                event_type: "pubsub.message.persist".to_string(),
                payload: serde_json::json!({
                    "subject": message.subject,
                    "message_id": message.id,
                    "payload_size": message.payload.len(),
                    "source": message.source,
                    "timestamp": message.timestamp,
                }),
            };

            if let Err(e) = publisher
                .publish(event, format!("pubsub-{}", self.node_id))
                .await
            {
                warn!("Failed to publish persistence event: {}", e);
            } else {
                self.stats.write().await.messages_persisted += 1;
            }
        }

        Ok(())
    }

    /// Get service statistics
    pub async fn get_stats(&self) -> PubSubStats {
        self.stats.read().await.clone()
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
            if subject_matches_pattern(&message.subject, pattern) {
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
}

impl<T, G> PubSubService<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Start the service
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting PubSub service");
        *self.state.write().await = ComponentState::Starting;

        // Start message processing loop
        if let Some(mut receiver) = self.message_channel.1.take() {
            let message_router = self.message_router.clone();
            let stats = self.stats.clone();

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
