//! Message routing for PubSub

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::subject::subject_matches_pattern;
use super::types::{DeliveryMode, Subscription};
use proven_topology::NodeId;

/// Type alias for subscription IDs
type SubscriptionIds = Vec<String>;
/// Type alias for pattern to subscription IDs mapping
type PatternSubscriptions = HashMap<String, SubscriptionIds>;
/// Type alias for queue group to pattern subscriptions mapping
type QueueGroupSubscriptions = HashMap<String, PatternSubscriptions>;

/// Routes messages to appropriate subscribers
#[derive(Clone)]
pub struct MessageRouter {
    /// Local subscriptions by ID
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    /// Pattern to subscription IDs mapping
    pattern_subs: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Queue groups: group name -> pattern -> subscription IDs
    queue_groups: Arc<RwLock<QueueGroupSubscriptions>>,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pattern_subs: Arc::new(RwLock::new(HashMap::new())),
            queue_groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a subscription
    pub async fn add_subscription(&self, subscription: Subscription) -> String {
        let sub_id = subscription.id.clone();
        let pattern = subscription.subject_pattern.clone();

        let mut subscriptions = self.subscriptions.write().await;
        let mut pattern_subs = self.pattern_subs.write().await;

        // Add to main subscription map
        subscriptions.insert(sub_id.clone(), subscription.clone());

        // Add to pattern index
        pattern_subs
            .entry(pattern.clone())
            .or_default()
            .insert(sub_id.clone());

        // Handle queue groups
        if let Some(queue_group) = &subscription.queue_group {
            let mut queue_groups = self.queue_groups.write().await;
            queue_groups
                .entry(queue_group.clone())
                .or_default()
                .entry(pattern)
                .or_default()
                .push(sub_id.clone());
        }

        sub_id
    }

    /// Remove a subscription
    pub async fn remove_subscription(&self, sub_id: &str) -> Option<Subscription> {
        let mut subscriptions = self.subscriptions.write().await;
        let mut pattern_subs = self.pattern_subs.write().await;

        if let Some(subscription) = subscriptions.remove(sub_id) {
            // Remove from pattern index
            if let Some(subs) = pattern_subs.get_mut(&subscription.subject_pattern) {
                subs.remove(sub_id);
                if subs.is_empty() {
                    pattern_subs.remove(&subscription.subject_pattern);
                }
            }

            // Remove from queue groups
            if let Some(queue_group) = &subscription.queue_group {
                let mut queue_groups = self.queue_groups.write().await;
                if let Some(group) = queue_groups.get_mut(queue_group) {
                    if let Some(subs) = group.get_mut(&subscription.subject_pattern) {
                        subs.retain(|id| id != sub_id);
                        if subs.is_empty() {
                            group.remove(&subscription.subject_pattern);
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
            if subject_matches_pattern(subject, pattern) {
                for sub_id in sub_ids {
                    if let Some(subscription) = subscriptions.get(sub_id) {
                        // Handle queue groups - only one subscriber per group gets the message
                        if let Some(queue_group) = &subscription.queue_group {
                            let group_key = format!("{queue_group}-{pattern}");
                            if handled_queue_groups.contains(&group_key) {
                                continue;
                            }
                            handled_queue_groups.insert(group_key);

                            // Load balance within queue group (simple round-robin)
                            if let Some(group) = queue_groups.get(queue_group)
                                && let Some(group_subs) = group.get(pattern)
                            {
                                // In a real implementation, we'd track round-robin state
                                // For now, just take the first active subscription
                                if let Some(first_sub_id) = group_subs.first()
                                    && let Some(sub) = subscriptions.get(first_sub_id)
                                {
                                    routed_subs.push(sub.clone());
                                }
                            }
                        } else {
                            // Regular subscription
                            routed_subs.push(subscription.clone());
                        }
                    }
                }
            }
        }

        routed_subs
    }

    /// Get subscriptions that need persistence
    pub async fn get_persistent_routes(&self, subject: &str) -> Vec<Subscription> {
        self.route_message(subject)
            .await
            .into_iter()
            .filter(|sub| sub.persist || sub.delivery_mode == DeliveryMode::Persistent)
            .collect()
    }

    /// Get all subscriptions for a node
    pub async fn get_node_subscriptions(&self, node_id: &NodeId) -> Vec<Subscription> {
        self.subscriptions
            .read()
            .await
            .values()
            .filter(|sub| &sub.node_id == node_id)
            .cloned()
            .collect()
    }

    /// Get subscription by ID
    pub async fn get_subscription(&self, sub_id: &str) -> Option<Subscription> {
        self.subscriptions.read().await.get(sub_id).cloned()
    }

    /// Clear all subscriptions
    pub async fn clear(&self) {
        self.subscriptions.write().await.clear();
        self.pattern_subs.write().await.clear();
        self.queue_groups.write().await.clear();
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
            subject_pattern: pattern.to_string(),
            node_id: node_id.clone(),
            persist: false,
            queue_group,
            delivery_mode: DeliveryMode::BestEffort,
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

    #[tokio::test]
    async fn test_persistent_routes() {
        let router = MessageRouter::new();

        let mut sub1 = create_test_subscription("sub1", "data.*", &NodeId::from_seed(1), None);
        sub1.persist = true;

        let mut sub2 = create_test_subscription("sub2", "data.*", &NodeId::from_seed(2), None);
        sub2.delivery_mode = DeliveryMode::Persistent;

        let sub3 = create_test_subscription("sub3", "data.*", &NodeId::from_seed(3), None);

        router.add_subscription(sub1).await;
        router.add_subscription(sub2).await;
        router.add_subscription(sub3).await;

        let persistent = router.get_persistent_routes("data.important").await;
        assert_eq!(persistent.len(), 2); // sub1 and sub2
    }
}
