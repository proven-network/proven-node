//! PubSub Manager implementation
//!
//! The main component that manages the publish-subscribe system.
//! Handles subscriptions, publishing, and coordination with the NetworkManager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::RwLock;
use proven_network::{NetworkManager, TopologyManager, Transport};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::subject::validate_subject;
use crate::network::messages::Message;
use proven_governance::Governance;
use proven_topology::NodeId;

use super::interest::InterestTracker;
use super::messages::{InterestUpdateAck as InterestUpdateAckStruct, PubSubMessage};
use super::router::MessageRouter;
use super::{PubSubError, PubSubResult};

// Type alias for complex subscription map
type SubscriptionMap = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<(String, Bytes)>>>>;

/// Pending interest update request
#[derive(Debug)]
struct PendingInterestUpdate {
    /// Response sender
    response_tx: oneshot::Sender<InterestUpdateAckStruct>,
    /// Target node ID
    #[allow(dead_code)]
    target_node_id: NodeId,
    /// Request timestamp for timeout tracking
    timestamp: Instant,
}

/// Handle for an active subscription
pub struct Subscription<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Unique subscription ID
    pub id: String,
    /// Subject pattern
    pub subject: String,
    /// Channel to receive messages
    pub receiver: mpsc::UnboundedReceiver<(String, Bytes)>,
    /// Handle to unsubscribe
    _handle: SubscriptionHandle<T, G>,
}

/// Internal handle to manage subscription lifecycle
struct SubscriptionHandle<T, G>
where
    T: Transport,
    G: Governance,
{
    subscription_id: String,
    manager: Arc<PubSubManagerInner<T, G>>,
}

impl<T, G> Drop for SubscriptionHandle<T, G>
where
    T: Transport,
    G: Governance,
{
    fn drop(&mut self) {
        // Automatically unsubscribe when dropped
        let manager = self.manager.clone();
        let sub_id = self.subscription_id.clone();
        tokio::spawn(async move {
            let _ = manager.unsubscribe_internal(&sub_id).await;
        });
    }
}

/// Internal state for PubSubManager
struct PubSubManagerInner<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Our node ID
    local_node_id: NodeId,
    /// Interest tracker
    interest_tracker: Arc<InterestTracker>,
    /// Message router
    message_router: Arc<MessageRouter>,
    /// Active local subscriptions
    subscriptions: SubscriptionMap,
    /// Pending requests waiting for responses
    pending_requests: Arc<RwLock<HashMap<Uuid, oneshot::Sender<Bytes>>>>,
    /// Network manager reference
    network_manager: Arc<NetworkManager<T, G>>,
    /// Topology manager for discovering peers
    topology_manager: Arc<TopologyManager<G>>,
    /// Pending interest update requests
    pending_interest_updates: Arc<RwLock<HashMap<Uuid, PendingInterestUpdate>>>,
}

/// PubSub manager
pub struct PubSubManager<T, G>
where
    T: Transport,
    G: Governance,
{
    inner: Arc<PubSubManagerInner<T, G>>,
}

// Manual Clone implementation that doesn't require T: Clone
impl<T, G> Clone for PubSubManager<T, G>
where
    T: Transport,
    G: Governance,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, G> PubSubManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new PubSubManager
    pub fn new(
        local_node_id: NodeId,
        network_manager: Arc<NetworkManager<T, G>>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        let interest_tracker = Arc::new(InterestTracker::new());
        let message_router = Arc::new(MessageRouter::new(
            local_node_id.clone(),
            interest_tracker.clone(),
        ));

        let inner = Arc::new(PubSubManagerInner {
            local_node_id,
            interest_tracker,
            message_router,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            network_manager,
            topology_manager,
            pending_interest_updates: Arc::new(RwLock::new(HashMap::new())),
        });

        Self { inner }
    }

    /// Publish a message to a subject
    pub async fn publish(&self, subject: &str, payload: Bytes) -> PubSubResult<()> {
        // Validate the subject (no wildcards allowed for publishing)
        validate_subject(subject).map_err(|e| PubSubError::InvalidSubject(e.to_string()))?;

        let message = PubSubMessage::Publish {
            subject: subject.to_string(),
            payload: payload.clone(),
            reply_to: None,
            message_id: Uuid::new_v4(),
        };

        // Route to interested remote nodes
        let target_nodes = self.inner.message_router.route_message(&message)?;

        // Send to each interested node
        for node_id in target_nodes {
            let net_msg = Message::PubSub(Box::new(message.clone()));

            if let Err(e) = self
                .inner
                .network_manager
                .send(node_id.clone(), net_msg)
                .await
            {
                warn!("Failed to send publish to {}: {}", node_id, e);
            }
        }

        // Deliver locally if we have subscribers
        if self.inner.message_router.should_deliver_locally(subject) {
            self.deliver_locally(subject, payload).await;
        }

        Ok(())
    }

    /// Subscribe to a subject pattern
    pub async fn subscribe(&self, subject: &str) -> PubSubResult<Subscription<T, G>> {
        let subscription_id = Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::unbounded_channel();

        // Add to local subscriptions
        self.inner
            .subscriptions
            .write()
            .insert(subscription_id.clone(), tx);
        self.inner
            .interest_tracker
            .add_local_subscription(subscription_id.clone(), subject.to_string())?;

        info!(
            "Local subscription created for subject '{}' with id {}",
            subject, subscription_id
        );

        // Announce interest to peers
        self.announce_interests().await?;

        let handle = SubscriptionHandle {
            subscription_id: subscription_id.clone(),
            manager: self.inner.clone(),
        };

        Ok(Subscription {
            id: subscription_id,
            subject: subject.to_string(),
            receiver: rx,
            _handle: handle,
        })
    }

    /// Make a request and wait for a response
    pub async fn request(
        &self,
        subject: &str,
        payload: Bytes,
        timeout_duration: Duration,
    ) -> PubSubResult<Bytes> {
        // Validate the subject (no wildcards allowed for requests)
        validate_subject(subject).map_err(|e| PubSubError::InvalidSubject(e.to_string()))?;

        let request_id = Uuid::new_v4();
        let reply_to = format!("_INBOX.{}", Uuid::new_v4());

        // Create a oneshot channel for the response
        let (tx, rx) = oneshot::channel();
        self.inner.pending_requests.write().insert(request_id, tx);

        // Subscribe to the reply subject
        let _reply_sub = self.subscribe(&reply_to).await?;

        // Send the request
        let message = PubSubMessage::Request {
            subject: subject.to_string(),
            payload,
            reply_to: reply_to.clone(),
            request_id,
        };

        // Route and send
        let target_nodes = self.inner.message_router.route_message(&message)?;
        if target_nodes.is_empty() && !self.inner.message_router.should_deliver_locally(subject) {
            return Err(PubSubError::NoResponders(subject.to_string()));
        }

        for node_id in target_nodes {
            let net_msg = Message::PubSub(Box::new(message.clone()));

            if let Err(e) = self
                .inner
                .network_manager
                .send(node_id.clone(), net_msg)
                .await
            {
                warn!("Failed to send request to {}: {}", node_id, e);
            }
        }

        // Wait for response with timeout
        match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(PubSubError::Internal("Response channel closed".to_string())),
            Err(_) => {
                self.inner.pending_requests.write().remove(&request_id);
                Err(PubSubError::RequestTimeout)
            }
        }
    }

    /// Handle incoming PubSub message from network
    pub async fn handle_message(
        &self,
        sender_node_id: NodeId,
        message: PubSubMessage,
        correlation_id: Option<Uuid>,
    ) -> PubSubResult<()> {
        match message {
            PubSubMessage::Subscribe {
                subject,
                subscription_id,
                node_id,
            } => {
                self.inner
                    .interest_tracker
                    .add_remote_interest(node_id, &subject)?;
                debug!(
                    "Added remote subscription {} for {}",
                    subscription_id, subject
                );
            }

            PubSubMessage::Unsubscribe {
                subject,
                subscription_id,
                node_id,
            } => {
                self.inner
                    .interest_tracker
                    .remove_remote_interest(&node_id, &subject)?;
                debug!(
                    "Removed remote subscription {} for {}",
                    subscription_id, subject
                );
            }

            PubSubMessage::Publish {
                ref subject,
                ref payload,
                ..
            } => {
                // Deliver to local subscribers
                if self.inner.message_router.should_deliver_locally(subject) {
                    self.deliver_locally(subject, payload.clone()).await;
                }

                // Forward to other interested nodes if needed
                let forward_nodes = self.inner.message_router.route_message(&message)?;
                for node_id in forward_nodes {
                    if node_id != sender_node_id {
                        let net_msg = Message::PubSub(Box::new(message.clone()));

                        let _ = self.inner.network_manager.send(node_id, net_msg).await;
                    }
                }
            }

            PubSubMessage::InterestUpdate { interests, node_id } => {
                // Update the interest tracker
                let update_result = self
                    .inner
                    .interest_tracker
                    .update_node_interests(node_id.clone(), interests);

                // Note: In the new NetworkManager design, responses should be returned
                // from handlers, not sent separately. The acknowledgment handling
                // happens at a higher level through the handler return value.

                if update_result.is_ok() {
                    debug!("Updated interests for node {}", sender_node_id);
                } else {
                    warn!("Failed to update interests for node {}", sender_node_id);
                }
            }

            PubSubMessage::Request {
                subject,
                payload,
                reply_to: _,
                request_id: _,
            } => {
                // Check if we should handle this request locally
                if self.inner.message_router.should_deliver_locally(&subject) {
                    // For now, just deliver to local subscribers
                    // In a full implementation, we'd have request handlers
                    self.deliver_locally(&subject, payload).await;
                }
            }

            PubSubMessage::Response {
                request_id,
                payload,
                ..
            } => {
                // Deliver response to waiting request
                if let Some(tx) = self.inner.pending_requests.write().remove(&request_id) {
                    let _ = tx.send(payload);
                }
            }

            PubSubMessage::InterestUpdateAck {
                node_id,
                success,
                error,
            } => {
                // Complete pending interest update request
                if let Some(corr_id) = correlation_id {
                    if let Some(pending) =
                        self.inner.pending_interest_updates.write().remove(&corr_id)
                    {
                        let ack = InterestUpdateAckStruct {
                            node_id,
                            success,
                            error,
                        };
                        let _ = pending.response_tx.send(ack);
                        debug!(
                            "Received interest update acknowledgment from {}",
                            sender_node_id
                        );
                    } else {
                        warn!("Received InterestUpdateAck with unknown correlation ID");
                    }
                } else {
                    warn!("Received InterestUpdateAck without correlation ID");
                }
            }
        }

        Ok(())
    }

    /// Announce our current interests to all peers
    async fn announce_interests(&self) -> PubSubResult<()> {
        let interests = self.inner.interest_tracker.get_local_patterns();
        info!("Announcing interests: {:?}", interests);

        let message = PubSubMessage::InterestUpdate {
            interests,
            node_id: self.inner.local_node_id.clone(),
        };

        // Get all peers from topology manager (not just connected ones)
        let all_peers = self.inner.topology_manager.get_all_peers().await;
        let target_nodes: Vec<NodeId> = all_peers
            .iter()
            .map(|node| NodeId::new(node.public_key()))
            .filter(|node_id| node_id != &self.inner.local_node_id)
            .collect();

        info!(
            "Announcing interests to {} nodes from topology",
            target_nodes.len()
        );

        let mut announcement_futures = Vec::new();

        for node_id in &target_nodes {
            let correlation_id = Uuid::new_v4();

            // Create pending request
            let (tx, rx) = oneshot::channel();
            let pending = PendingInterestUpdate {
                response_tx: tx,
                target_node_id: node_id.clone(),
                timestamp: Instant::now(),
            };

            self.inner
                .pending_interest_updates
                .write()
                .insert(correlation_id, pending);

            let net_msg = Message::PubSub(Box::new(message.clone()));

            let network_manager = self.inner.network_manager.clone();
            let node_id_clone = node_id.clone();

            // Create future for sending and waiting for response
            let future = async move {
                // Use request method for messages that expect responses
                match network_manager
                    .request(node_id_clone.clone(), net_msg, Duration::from_secs(5))
                    .await
                {
                    Ok(_response_bytes) => {
                        // Response will be handled through the rx channel
                    }
                    Err(e) => {
                        warn!("Failed to send interest update to {}: {}", node_id_clone, e);
                        return Err(PubSubError::Network(format!("Failed to send: {e}")));
                    }
                }

                // Wait for acknowledgment with timeout
                match timeout(Duration::from_secs(5), rx).await {
                    Ok(Ok(ack)) => {
                        if ack.success {
                            debug!("Interest update acknowledged by {}", node_id_clone);
                            Ok(())
                        } else {
                            warn!(
                                "Interest update rejected by {}: {:?}",
                                node_id_clone, ack.error
                            );
                            Err(PubSubError::Network(
                                ack.error.unwrap_or_else(|| "Unknown error".to_string()),
                            ))
                        }
                    }
                    Ok(Err(_)) => {
                        warn!("Interest update channel closed for {}", node_id_clone);
                        Err(PubSubError::Network("Channel closed".to_string()))
                    }
                    Err(_) => {
                        warn!("Interest update timeout for {}", node_id_clone);
                        Err(PubSubError::Network("Timeout".to_string()))
                    }
                }
            };

            announcement_futures.push(future);
        }

        // Wait for all announcements to complete
        let results = futures::future::join_all(announcement_futures).await;

        // Clean up any remaining pending requests
        let mut pending = self.inner.pending_interest_updates.write();
        pending.retain(|_, req| req.timestamp.elapsed() < Duration::from_secs(10));

        // Collect failed nodes for potential retry
        let failed_nodes: Vec<_> = target_nodes
            .into_iter()
            .zip(results.iter())
            .filter_map(
                |(node_id, result)| {
                    if result.is_err() { Some(node_id) } else { None }
                },
            )
            .collect();

        // Log results
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let failed = failed_nodes.len();

        if failed > 0 {
            warn!(
                "Interest announcement completed: {} successful, {} failed",
                successful, failed
            );

            // Schedule retry for failed nodes
            if !failed_nodes.is_empty() {
                let manager = self.clone();
                tokio::spawn(async move {
                    // Wait before retrying
                    tokio::time::sleep(Duration::from_secs(10)).await;

                    info!(
                        "Retrying interest announcement for {} failed nodes",
                        failed_nodes.len()
                    );

                    // Only retry to failed nodes
                    if let Err(e) = manager.announce_interests_to_nodes(failed_nodes).await {
                        warn!("Retry of interest announcement failed: {}", e);
                    }
                });
            }
        } else {
            info!(
                "Interest announcement completed: all {} successful",
                successful
            );
        }

        Ok(())
    }

    /// Announce interests to specific nodes (used for retries)
    async fn announce_interests_to_nodes(&self, target_nodes: Vec<NodeId>) -> PubSubResult<()> {
        let interests = self.inner.interest_tracker.get_local_patterns();

        if interests.is_empty() || target_nodes.is_empty() {
            return Ok(());
        }

        let message = PubSubMessage::InterestUpdate {
            interests,
            node_id: self.inner.local_node_id.clone(),
        };

        let mut announcement_futures = Vec::new();

        for node_id in target_nodes {
            let correlation_id = Uuid::new_v4();

            // Create pending request
            let (tx, rx) = oneshot::channel();
            let pending = PendingInterestUpdate {
                response_tx: tx,
                target_node_id: node_id.clone(),
                timestamp: Instant::now(),
            };

            self.inner
                .pending_interest_updates
                .write()
                .insert(correlation_id, pending);

            let net_msg = Message::PubSub(Box::new(message.clone()));

            let network_manager = self.inner.network_manager.clone();
            let node_id_clone = node_id.clone();

            // Create future for sending and waiting for response
            let future = async move {
                // Use request method for messages that expect responses
                match network_manager
                    .request(node_id_clone.clone(), net_msg, Duration::from_secs(5))
                    .await
                {
                    Ok(_response_bytes) => {
                        // Response will be handled through the rx channel
                    }
                    Err(e) => {
                        return Err(PubSubError::Network(format!("Failed to send: {e}")));
                    }
                }

                // Wait for acknowledgment with timeout
                match timeout(Duration::from_secs(5), rx).await {
                    Ok(Ok(ack)) => {
                        if ack.success {
                            Ok(())
                        } else {
                            Err(PubSubError::Network(
                                ack.error.unwrap_or_else(|| "Unknown error".to_string()),
                            ))
                        }
                    }
                    Ok(Err(_)) => Err(PubSubError::Network("Channel closed".to_string())),
                    Err(_) => Err(PubSubError::Network("Timeout".to_string())),
                }
            };

            announcement_futures.push(future);
        }

        // Wait for all announcements
        let _ = futures::future::join_all(announcement_futures).await;

        Ok(())
    }

    /// Deliver a message to local subscribers
    async fn deliver_locally(&self, subject: &str, payload: Bytes) {
        let subs = self.inner.subscriptions.read();
        let interested_sub_ids = self
            .inner
            .interest_tracker
            .get_local_subscriptions_for_subject(subject);

        for sub_id in interested_sub_ids {
            if let Some(tx) = subs.get(&sub_id) {
                let _ = tx.send((subject.to_string(), payload.clone()));
            }
        }
    }

    /// Handle peer disconnection
    pub async fn handle_peer_disconnect(&self, node_id: &NodeId) {
        self.inner.interest_tracker.remove_node(node_id);
        info!("Removed interests for disconnected peer {}", node_id);
    }
}

impl<T, G> PubSubManagerInner<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Internal unsubscribe
    async fn unsubscribe_internal(&self, subscription_id: &str) -> PubSubResult<()> {
        self.subscriptions.write().remove(subscription_id);

        if let Some(pattern) = self
            .interest_tracker
            .remove_local_subscription(subscription_id)
        {
            // Check if we still have other subscriptions for this pattern
            let still_interested = self
                .interest_tracker
                .get_local_patterns()
                .contains(&pattern);

            if !still_interested {
                // Announce updated interests
                // This would trigger announce_interests() in a full implementation
                debug!("Removed last subscription for pattern {}", pattern);
            }
        }

        Ok(())
    }
}
