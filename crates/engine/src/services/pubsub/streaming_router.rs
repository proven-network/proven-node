//! Streaming message router for PubSub
//!
//! This router provides direct streaming channels for efficient message delivery
//! without the overhead of broadcast channels.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use super::types::Subscription;
use crate::foundation::Message;
use crate::foundation::types::subject_matches_pattern;

/// Type alias for subscription IDs
type SubscriptionIds = Vec<String>;
/// Type alias for pattern to subscription IDs mapping
type PatternSubscriptions = HashMap<String, SubscriptionIds>;
/// Type alias for queue group to pattern subscriptions mapping
type QueueGroupSubscriptions = HashMap<String, PatternSubscriptions>;

/// Message channel for direct streaming delivery
#[derive(Clone)]
pub struct MessageChannel {
    /// Direct streaming channel
    sender: flume::Sender<Message>,
}

impl MessageChannel {
    /// Create a new message channel
    pub fn new(sender: flume::Sender<Message>) -> Self {
        Self { sender }
    }

    /// Send a message through the channel
    pub async fn send(&self, msg: Message) -> Result<(), String> {
        self.sender.send_async(msg).await.map_err(|e| e.to_string())
    }

    /// Check if the channel is closed
    pub fn is_closed(&self) -> bool {
        self.sender.is_disconnected()
    }
}

/// Subscription with streaming channel
#[derive(Clone)]
pub struct StreamingSubscription {
    /// Subscription metadata
    pub subscription: Subscription,
    /// Channel for message delivery
    pub channel: MessageChannel,
}

/// Streaming message router for efficient message delivery
#[derive(Clone, Default)]
pub struct StreamingMessageRouter {
    /// Local subscriptions by ID
    subscriptions: Arc<RwLock<HashMap<String, StreamingSubscription>>>,
    /// Pattern to subscription IDs mapping
    pattern_subs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Queue groups: group name -> pattern -> subscription IDs
    queue_groups: Arc<RwLock<QueueGroupSubscriptions>>,
    /// Subscription IDs by exact subject (optimization for non-wildcard subscriptions)
    exact_subject_subs: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Round-robin counters for queue groups
    queue_group_counters: Arc<RwLock<HashMap<String, HashMap<String, usize>>>>,
}

impl StreamingMessageRouter {
    /// Create a new streaming message router
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pattern_subs: Arc::new(RwLock::new(HashMap::new())),
            queue_groups: Arc::new(RwLock::new(HashMap::new())),
            exact_subject_subs: Arc::new(RwLock::new(HashMap::new())),
            queue_group_counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a subscription with a streaming channel
    pub async fn add_streaming_subscription(
        &self,
        sub_id: String,
        subscription: Subscription,
        sender: flume::Sender<Message>,
    ) {
        let channel = MessageChannel::new(sender);
        self.add_subscription(sub_id, subscription, channel).await;
    }

    /// Add a subscription
    async fn add_subscription(
        &self,
        sub_id: String,
        subscription: Subscription,
        channel: MessageChannel,
    ) {
        let pattern = subscription.subject_pattern.as_str();

        // Add to main subscription map
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(
            sub_id.clone(),
            StreamingSubscription {
                subscription: subscription.clone(),
                channel,
            },
        );
        drop(subscriptions);

        // Add to pattern index
        let mut pattern_subs = self.pattern_subs.write().await;
        pattern_subs
            .entry(pattern.to_string())
            .or_insert_with(HashSet::new)
            .insert(sub_id.clone());
        drop(pattern_subs);

        // Handle exact subjects (no wildcards)
        if !pattern.contains('*') && !pattern.contains('>') {
            let mut exact_subs = self.exact_subject_subs.write().await;
            exact_subs
                .entry(pattern.to_string())
                .or_insert_with(Vec::new)
                .push(sub_id.clone());
        }

        // Handle queue groups
        if let Some(ref queue_group) = subscription.queue_group {
            let mut queue_groups = self.queue_groups.write().await;
            queue_groups
                .entry(queue_group.clone())
                .or_insert_with(HashMap::new)
                .entry(pattern.to_string())
                .or_insert_with(Vec::new)
                .push(sub_id.clone());
        }

        debug!(
            "Added subscription {} for pattern {} (queue_group: {:?})",
            sub_id, pattern, subscription.queue_group
        );
    }

    /// Remove a subscription
    pub async fn remove_subscription(&self, sub_id: &str) -> Option<Subscription> {
        let mut subscriptions = self.subscriptions.write().await;

        if let Some(sub_with_channel) = subscriptions.remove(sub_id) {
            let pattern = sub_with_channel.subscription.subject_pattern.as_str();

            // Remove from pattern index
            let mut pattern_subs = self.pattern_subs.write().await;
            if let Some(subs) = pattern_subs.get_mut(pattern) {
                subs.remove(sub_id);
                if subs.is_empty() {
                    pattern_subs.remove(pattern);
                }
            }
            drop(pattern_subs);

            // Remove from exact subject index
            if !pattern.contains('*') && !pattern.contains('>') {
                let mut exact_subs = self.exact_subject_subs.write().await;
                if let Some(subs) = exact_subs.get_mut(pattern) {
                    subs.retain(|id| id != sub_id);
                    if subs.is_empty() {
                        exact_subs.remove(pattern);
                    }
                }
            }

            // Remove from queue groups
            if let Some(ref queue_group) = sub_with_channel.subscription.queue_group {
                let mut queue_groups = self.queue_groups.write().await;
                if let Some(patterns) = queue_groups.get_mut(queue_group) {
                    if let Some(subs) = patterns.get_mut(pattern) {
                        subs.retain(|id| id != sub_id);
                        if subs.is_empty() {
                            patterns.remove(pattern);
                        }
                    }
                    if patterns.is_empty() {
                        queue_groups.remove(queue_group);
                    }
                }
            }

            debug!("Removed subscription {}", sub_id);
            Some(sub_with_channel.subscription)
        } else {
            None
        }
    }

    /// Route a message and return number of local subscribers
    pub async fn route(&self, message: &Message) -> usize {
        let subject = message.subject().unwrap_or("");
        let mut delivered_count = 0;

        // First check exact matches (optimization)
        let exact_subs = self.exact_subject_subs.read().await;
        if let Some(sub_ids) = exact_subs.get(subject) {
            let subscriptions = self.subscriptions.read().await;
            for sub_id in sub_ids {
                if let Some(sub) = subscriptions.get(sub_id)
                    && !sub.channel.is_closed()
                    && sub.channel.send(message.clone()).await.is_ok()
                {
                    delivered_count += 1;
                }
            }
        }
        drop(exact_subs);

        // Then check pattern matches
        let mut queue_group_deliveries = HashMap::new();
        let pattern_subs = self.pattern_subs.read().await;
        let subscriptions = self.subscriptions.read().await;

        for (pattern, sub_ids) in pattern_subs.iter() {
            if subject_matches_pattern(subject, pattern) {
                for sub_id in sub_ids {
                    if let Some(sub) = subscriptions.get(sub_id) {
                        // Handle queue groups
                        if let Some(ref queue_group) = sub.subscription.queue_group {
                            let group_key = format!("{queue_group}-{pattern}");
                            queue_group_deliveries
                                .entry(group_key)
                                .or_insert_with(Vec::new)
                                .push(sub_id.clone());
                        } else {
                            // Regular subscription
                            if !sub.channel.is_closed()
                                && sub.channel.send(message.clone()).await.is_ok()
                            {
                                delivered_count += 1;
                            }
                        }
                    }
                }
            }
        }
        drop(pattern_subs);

        // Handle queue group deliveries (round-robin)
        if !queue_group_deliveries.is_empty() {
            let mut counters = self.queue_group_counters.write().await;

            for (group_key, sub_ids) in queue_group_deliveries {
                let parts: Vec<&str> = group_key.splitn(2, '-').collect();
                let pattern = parts.get(1).unwrap_or(&"").to_string();

                let counter = counters
                    .entry(pattern.clone())
                    .or_insert_with(HashMap::new)
                    .entry(group_key.clone())
                    .or_insert(0);

                // Round-robin delivery
                let mut attempts = 0;
                while attempts < sub_ids.len() {
                    let idx = *counter % sub_ids.len();
                    if let Some(sub_id) = sub_ids.get(idx)
                        && let Some(sub) = subscriptions.get(sub_id)
                        && !sub.channel.is_closed()
                        && sub.channel.send(message.clone()).await.is_ok()
                    {
                        delivered_count += 1;
                        *counter = (*counter + 1) % sub_ids.len();
                        break;
                    }
                    *counter = (*counter + 1) % sub_ids.len();
                    attempts += 1;
                }
            }
        }

        delivered_count
    }

    /// Get all subscriptions
    pub async fn get_subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .await
            .values()
            .map(|s| s.subscription.clone())
            .collect()
    }

    /// Get subscription by ID
    pub async fn get_subscription(&self, sub_id: &str) -> Option<Subscription> {
        self.subscriptions
            .read()
            .await
            .get(sub_id)
            .map(|s| s.subscription.clone())
    }

    /// Get all subscriptions for a node
    pub async fn get_node_subscriptions(
        &self,
        node_id: &proven_topology::NodeId,
    ) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .await
            .values()
            .filter(|s| &s.subscription.node_id == node_id)
            .map(|s| s.subscription.clone())
            .collect()
    }

    /// Clear all subscriptions
    pub async fn clear(&self) {
        self.subscriptions.write().await.clear();
        self.pattern_subs.write().await.clear();
        self.queue_groups.write().await.clear();
        self.exact_subject_subs.write().await.clear();
        self.queue_group_counters.write().await.clear();
    }
}
