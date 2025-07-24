//! Message routing for PubSub

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{RwLock, broadcast};

use super::types::{PubSubNetworkMessage, Subscription};
use crate::foundation::types::{SubjectPattern, subject_matches_pattern};
use proven_topology::NodeId;

/// Type alias for subscription IDs
type SubscriptionIds = Vec<String>;
/// Type alias for pattern to subscription IDs mapping
type PatternSubscriptions = HashMap<String, SubscriptionIds>;
/// Type alias for queue group to pattern subscriptions mapping
type QueueGroupSubscriptions = HashMap<String, PatternSubscriptions>;

/// Subscription with broadcast channel
#[derive(Clone)]
pub struct SubscriptionWithChannel {
    /// Subscription metadata
    pub subscription: Subscription,
    /// Broadcast sender for this subscription
    pub sender: broadcast::Sender<PubSubNetworkMessage>,
}

/// Routes messages to appropriate subscribers
#[derive(Clone)]
pub struct MessageRouter {
    /// Local subscriptions by ID with their broadcast channels
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionWithChannel>>>,
    /// Pattern to subscription IDs mapping
    pattern_subs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Queue groups: group name -> pattern -> subscription IDs
    queue_groups: Arc<RwLock<QueueGroupSubscriptions>>,
    /// Broadcast channels by exact subject (optimization for non-wildcard subscriptions)
    exact_subject_channels:
        Arc<RwLock<HashMap<String, Vec<broadcast::Sender<PubSubNetworkMessage>>>>>,
    /// Round-robin counters for queue groups (group_name -> pattern -> counter)
    queue_group_counters: Arc<RwLock<HashMap<String, HashMap<String, usize>>>>,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pattern_subs: Arc::new(RwLock::new(HashMap::new())),
            queue_groups: Arc::new(RwLock::new(HashMap::new())),
            exact_subject_channels: Arc::new(RwLock::new(HashMap::new())),
            queue_group_counters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a subscription
    pub async fn add_subscription(&self, subscription: Subscription) {
        let sub_id = subscription.id.clone();
        let pattern = subscription.subject_pattern.clone();

        // Create broadcast channel for this subscription
        let (sender, _receiver) = broadcast::channel(1000); // Buffer size of 1000 messages

        let mut subscriptions = self.subscriptions.write().await;
        let mut pattern_subs = self.pattern_subs.write().await;

        // Add to main subscription map with channel
        subscriptions.insert(
            sub_id.clone(),
            SubscriptionWithChannel {
                subscription: subscription.clone(),
                sender: sender.clone(),
            },
        );

        // Add to pattern index
        pattern_subs
            .entry(pattern.as_ref().to_string())
            .or_default()
            .insert(sub_id.clone());

        // Optimize exact subject matches
        let pattern_str = pattern.as_ref();
        if !pattern_str.contains('*') && !pattern_str.contains('>') {
            let mut exact_channels = self.exact_subject_channels.write().await;
            exact_channels
                .entry(pattern_str.to_string())
                .or_default()
                .push(sender);
        }

        // Handle queue groups
        if let Some(queue_group) = &subscription.queue_group {
            let mut queue_groups = self.queue_groups.write().await;
            queue_groups
                .entry(queue_group.clone())
                .or_default()
                .entry(pattern.as_ref().to_string())
                .or_default()
                .push(sub_id.clone());
        }
    }

    /// Remove a subscription
    pub async fn remove_subscription(&self, sub_id: &str) -> Option<Subscription> {
        let mut subscriptions = self.subscriptions.write().await;
        let mut pattern_subs = self.pattern_subs.write().await;

        if let Some(sub_with_channel) = subscriptions.remove(sub_id) {
            let subscription = sub_with_channel.subscription;
            let pattern = &subscription.subject_pattern;

            // Remove from pattern index
            let pattern_str = pattern.as_ref();
            if let Some(subs) = pattern_subs.get_mut(pattern_str) {
                subs.remove(sub_id);
                if subs.is_empty() {
                    pattern_subs.remove(pattern_str);
                }
            }

            // Remove from exact subject channels if applicable
            let pattern_str = pattern.as_ref();
            if !pattern_str.contains('*') && !pattern_str.contains('>') {
                let mut exact_channels = self.exact_subject_channels.write().await;
                if let Some(channels) = exact_channels.get_mut(pattern_str) {
                    // Remove this specific sender (by checking receiver count)
                    channels.retain(|sender| {
                        // This is a simplified check - in production we'd need a better way
                        // to identify the specific sender
                        sender.receiver_count() > 0
                    });
                    if channels.is_empty() {
                        exact_channels.remove(pattern_str);
                    }
                }
            }

            // Remove from queue groups
            if let Some(queue_group) = &subscription.queue_group {
                let mut queue_groups = self.queue_groups.write().await;
                if let Some(group) = queue_groups.get_mut(queue_group) {
                    let pattern_str = pattern.as_ref();
                    if let Some(subs) = group.get_mut(pattern_str) {
                        subs.retain(|id| id != sub_id);
                        if subs.is_empty() {
                            group.remove(pattern_str);
                        }
                    }
                    if group.is_empty() {
                        queue_groups.remove(queue_group);
                    }
                }
            }

            Some(subscription)
        } else {
            None
        }
    }

    /// Route a message to matching subscriptions
    pub async fn route_message(&self, subject: &str) -> Vec<Subscription> {
        let subscriptions = self.subscriptions.read().await;
        let pattern_subs = self.pattern_subs.read().await;
        let queue_groups = self.queue_groups.read().await;

        let mut routed_subs = Vec::new();
        let mut handled_queue_groups = HashSet::new();

        // Find all matching patterns
        for (pattern, sub_ids) in pattern_subs.iter() {
            if subject_matches_pattern(subject, pattern.as_ref()) {
                for sub_id in sub_ids {
                    if let Some(sub_with_channel) = subscriptions.get(sub_id) {
                        let subscription = &sub_with_channel.subscription;
                        // Handle queue groups - only one subscriber per group gets the message
                        if let Some(queue_group) = &subscription.queue_group {
                            let group_key = format!("{queue_group}-{pattern}");
                            if handled_queue_groups.contains(&group_key) {
                                continue;
                            }
                            handled_queue_groups.insert(group_key);

                            // Load balance within queue group
                            if let Some(group) = queue_groups.get(queue_group)
                                && let Some(group_subs) = group.get(pattern)
                                && !group_subs.is_empty()
                            {
                                // Get round-robin counter for this queue group and pattern
                                let mut counters = self.queue_group_counters.write().await;
                                let group_counters =
                                    counters.entry(queue_group.clone()).or_default();
                                let counter = group_counters.entry(pattern.clone()).or_default();

                                // Select next subscriber in round-robin fashion
                                let idx = *counter % group_subs.len();
                                if let Some(sub_id) = group_subs.get(idx)
                                    && let Some(sub) = subscriptions.get(sub_id)
                                {
                                    routed_subs.push(sub.subscription.clone());
                                    *counter = (*counter + 1) % group_subs.len();
                                }
                            }
                        } else {
                            // Regular subscription
                            routed_subs.push(sub_with_channel.subscription.clone());
                        }
                    }
                }
            }
        }

        routed_subs
    }

    /// Route a message and return number of local subscribers
    pub async fn route(&self, message: &PubSubNetworkMessage) -> usize {
        let subject = message.subject.as_str();
        let mut delivered_count = 0;

        // Check exact subject matches first (optimization)
        {
            let exact_channels = self.exact_subject_channels.read().await;
            if let Some(channels) = exact_channels.get(subject) {
                for sender in channels {
                    if sender.send(message.clone()).is_ok() {
                        delivered_count += 1;
                    }
                }
            }
        }

        // Check pattern matches
        let subscriptions = self.subscriptions.read().await;
        let pattern_subs = self.pattern_subs.read().await;
        let queue_groups = self.queue_groups.read().await;

        let mut handled_queue_groups = HashSet::new();
        let mut handled_exact = HashSet::new();

        for (pattern, sub_ids) in pattern_subs.iter() {
            // Skip exact matches we already handled
            if !pattern.contains('*') && !pattern.contains('>') {
                continue;
            }

            if subject_matches_pattern(subject, pattern) {
                for sub_id in sub_ids {
                    if let Some(sub_with_channel) = subscriptions.get(sub_id) {
                        let subscription = &sub_with_channel.subscription;

                        // Handle queue groups - only one subscriber per group gets the message
                        if let Some(queue_group) = &subscription.queue_group {
                            let group_key = format!("{queue_group}-{pattern}");
                            if handled_queue_groups.contains(&group_key) {
                                continue;
                            }
                            handled_queue_groups.insert(group_key);

                            // Load balance within queue group
                            if let Some(group) = queue_groups.get(queue_group)
                                && let Some(group_subs) = group.get(pattern)
                                && !group_subs.is_empty()
                            {
                                // Get round-robin counter for this queue group and pattern
                                let mut counters = self.queue_group_counters.write().await;
                                let group_counters =
                                    counters.entry(queue_group.clone()).or_default();
                                let counter = group_counters.entry(pattern.clone()).or_default();

                                // Find next valid subscriber using round-robin
                                let mut delivered = false;
                                let start_idx = *counter;
                                for i in 0..group_subs.len() {
                                    let idx = (start_idx + i) % group_subs.len();
                                    if let Some(sub_id) = group_subs.get(idx)
                                        && let Some(sub_with_chan) = subscriptions.get(sub_id)
                                        && sub_with_chan.sender.send(message.clone()).is_ok()
                                    {
                                        delivered_count += 1;
                                        delivered = true;
                                        *counter = (idx + 1) % group_subs.len();
                                        break;
                                    }
                                }
                                // If no subscribers could receive, reset counter
                                if !delivered {
                                    *counter = 0;
                                }
                            }
                        } else if !handled_exact.contains(sub_id) {
                            // Regular subscription (avoid double delivery to exact matches)
                            if sub_with_channel.sender.send(message.clone()).is_ok() {
                                delivered_count += 1;
                                handled_exact.insert(sub_id);
                            }
                        }
                    }
                }
            }
        }

        delivered_count
    }

    /// Get all subscriptions for a node
    pub async fn get_node_subscriptions(&self, node_id: &NodeId) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .await
            .values()
            .filter(|sub| &sub.subscription.node_id == node_id)
            .map(|sub| sub.subscription.clone())
            .collect()
    }

    /// Get subscription by ID
    pub async fn get_subscription(&self, sub_id: &str) -> Option<Subscription> {
        self.subscriptions
            .read()
            .await
            .get(sub_id)
            .map(|sub| sub.subscription.clone())
    }

    /// Get a receiver for a subscription
    pub async fn get_receiver(
        &self,
        sub_id: &str,
    ) -> Option<broadcast::Receiver<PubSubNetworkMessage>> {
        self.subscriptions
            .read()
            .await
            .get(sub_id)
            .map(|sub| sub.sender.subscribe())
    }

    /// Clear all subscriptions
    pub async fn clear(&self) {
        self.subscriptions.write().await.clear();
        self.pattern_subs.write().await.clear();
        self.queue_groups.write().await.clear();
        self.exact_subject_channels.write().await.clear();
        self.queue_group_counters.write().await.clear();
    }
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn create_test_subscription(
        id: &str,
        pattern: &str,
        node_id: &NodeId,
        queue_group: Option<String>,
    ) -> Subscription {
        Subscription {
            id: id.to_string(),
            subject_pattern: SubjectPattern::new(pattern.to_string()).unwrap(),
            node_id: node_id.clone(),
            queue_group,
            created_at: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn test_basic_routing() {
        let router = MessageRouter::new();

        // Add subscriptions
        let sub1 = create_test_subscription("sub1", "metrics.*", &NodeId::from_seed(1), None);
        let sub2 = create_test_subscription("sub2", "metrics.cpu", &NodeId::from_seed(2), None);
        let sub3 = create_test_subscription("sub3", "events.>", &NodeId::from_seed(1), None);

        router.add_subscription(sub1).await;
        router.add_subscription(sub2).await;
        router.add_subscription(sub3).await;

        // Test routing
        let routes = router.route_message("metrics.cpu").await;
        assert_eq!(routes.len(), 2); // sub1 and sub2 match

        let routes = router.route_message("events.user.login").await;
        assert_eq!(routes.len(), 1); // only sub3 matches
    }

    #[tokio::test]
    async fn test_queue_groups() {
        let router = MessageRouter::new();

        // Add queue group subscriptions
        let sub1 = create_test_subscription(
            "sub1",
            "work.*",
            &NodeId::from_seed(1),
            Some("workers".to_string()),
        );
        let sub2 = create_test_subscription(
            "sub2",
            "work.*",
            &NodeId::from_seed(2),
            Some("workers".to_string()),
        );
        let sub3 = create_test_subscription("sub3", "work.*", &NodeId::from_seed(3), None);

        router.add_subscription(sub1).await;
        router.add_subscription(sub2).await;
        router.add_subscription(sub3).await;

        // Test routing - should get one from queue group and the regular subscription
        let routes = router.route_message("work.task").await;
        assert_eq!(routes.len(), 2); // One from queue group, one regular

        let queue_group_count = routes
            .iter()
            .filter(|sub| sub.queue_group.is_some())
            .count();
        assert_eq!(queue_group_count, 1);
    }
}
