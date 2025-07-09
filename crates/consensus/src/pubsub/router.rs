//! Message routing logic for PubSub
//!
//! Handles the routing of published messages to interested peers
//! based on subject pattern matching.

use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use super::PubSubResult;
use super::interest::InterestTracker;
use super::messages::PubSubMessage;
use super::subject::subject_matches_pattern;
use crate::NodeId;

/// Routes messages to interested nodes based on subject patterns
#[derive(Debug, Clone)]
pub struct MessageRouter {
    /// Our local node ID
    local_node_id: NodeId,

    /// Interest tracker
    interest_tracker: Arc<InterestTracker>,

    /// Track recently seen message IDs to prevent loops
    /// This is a simple deduplication mechanism
    seen_messages: Arc<RwLock<HashSet<Uuid>>>,

    /// Maximum number of seen messages to track
    max_seen_messages: usize,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new(local_node_id: NodeId, interest_tracker: Arc<InterestTracker>) -> Self {
        Self {
            local_node_id,
            interest_tracker,
            seen_messages: Arc::new(RwLock::new(HashSet::new())),
            max_seen_messages: 10000, // Track last 10k messages
        }
    }

    /// Route a message to all interested remote nodes
    /// Returns the set of nodes that should receive this message
    pub fn route_message(&self, message: &PubSubMessage) -> PubSubResult<HashSet<NodeId>> {
        match message {
            PubSubMessage::Publish {
                subject,
                message_id,
                ..
            }
            | PubSubMessage::Request {
                subject,
                request_id: message_id,
                ..
            } => {
                // Check for duplicate messages
                if !self.mark_seen(*message_id) {
                    trace!("Ignoring duplicate message {}", message_id);
                    return Ok(HashSet::new());
                }

                // Find all interested nodes
                let mut interested = self.interest_tracker.find_interested_nodes(subject);

                // Remove ourselves from the set (we don't need to send to ourselves)
                interested.remove(&self.local_node_id);

                debug!(
                    "Routing message on subject '{}' to {} nodes",
                    subject,
                    interested.len()
                );

                Ok(interested)
            }

            PubSubMessage::Response { request_id: _, .. } => {
                // Responses are not routed based on subject patterns
                // They should be sent directly to the requester
                trace!("Response messages are not subject-routed");
                Ok(HashSet::new())
            }

            PubSubMessage::InterestUpdate { node_id, .. } => {
                // Interest updates go to all known nodes
                let mut all_nodes = self.interest_tracker.get_all_interested_nodes();
                all_nodes.remove(&self.local_node_id);
                all_nodes.remove(node_id); // Don't send back to sender

                debug!("Broadcasting interest update to {} nodes", all_nodes.len());
                Ok(all_nodes)
            }

            _ => {
                // Subscribe/Unsubscribe are handled differently
                Ok(HashSet::new())
            }
        }
    }

    /// Check if a message should be delivered locally based on subscriptions
    pub fn should_deliver_locally(&self, subject: &str) -> bool {
        let local_patterns = self.interest_tracker.get_local_patterns();

        for pattern in local_patterns {
            if subject_matches_pattern(subject, &pattern) {
                return true;
            }
        }

        false
    }

    /// Mark a message as seen and return whether it's new
    fn mark_seen(&self, message_id: Uuid) -> bool {
        let mut seen = self.seen_messages.write();

        // Clean up old messages if we're at capacity
        if seen.len() >= self.max_seen_messages {
            // In a production system, we'd want a more sophisticated
            // eviction strategy (like LRU), but for now we'll just clear
            seen.clear();
            warn!("Cleared seen message cache due to size limit");
        }

        seen.insert(message_id)
    }

    /// Clear the seen messages cache
    #[allow(dead_code)]
    pub fn clear_seen_messages(&self) {
        self.seen_messages.write().clear();
    }

    /// Get the number of currently tracked seen messages
    #[allow(dead_code)]
    pub fn seen_message_count(&self) -> usize {
        self.seen_messages.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_message_routing() {
        let local_node = NodeId::from_seed(0);
        let interest_tracker = Arc::new(InterestTracker::new());
        let router = MessageRouter::new(local_node.clone(), interest_tracker.clone());

        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        // Set up interests
        interest_tracker
            .add_remote_interest(node1.clone(), "foo.*")
            .unwrap();
        interest_tracker
            .add_remote_interest(node2.clone(), "foo.bar")
            .unwrap();
        interest_tracker
            .add_local_subscription("sub1".to_string(), "foo.>".to_string())
            .unwrap();

        // Route a message
        let message = PubSubMessage::Publish {
            subject: "foo.bar".to_string(),
            payload: Bytes::from("test"),
            reply_to: None,
            message_id: Uuid::new_v4(),
        };

        let nodes = router.route_message(&message).unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&node1));
        assert!(nodes.contains(&node2));

        // Check local delivery
        assert!(router.should_deliver_locally("foo.bar"));
        assert!(router.should_deliver_locally("foo.baz"));
        assert!(!router.should_deliver_locally("bar.foo"));
    }

    #[test]
    fn test_duplicate_detection() {
        let local_node = NodeId::from_seed(0);
        let interest_tracker = Arc::new(InterestTracker::new());
        let router = MessageRouter::new(local_node, interest_tracker.clone());

        let node1 = NodeId::from_seed(1);
        interest_tracker
            .add_remote_interest(node1, "test.*")
            .unwrap();

        let message_id = Uuid::new_v4();
        let message = PubSubMessage::Publish {
            subject: "test.foo".to_string(),
            payload: Bytes::from("test"),
            reply_to: None,
            message_id,
        };

        // First time should route
        let nodes = router.route_message(&message).unwrap();
        assert_eq!(nodes.len(), 1);

        // Second time should not route (duplicate)
        let nodes = router.route_message(&message).unwrap();
        assert_eq!(nodes.len(), 0);
    }
}
