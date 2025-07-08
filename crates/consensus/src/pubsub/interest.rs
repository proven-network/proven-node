//! Interest tracking for PubSub
//!
//! Manages which nodes are interested in which subject patterns,
//! enabling efficient message routing without consensus overhead.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, trace};

use super::subject::{subject_matches_pattern, validate_subject_pattern};
use super::{PubSubError, PubSubResult};
use crate::NodeId;

/// Tracks interest subscriptions across the cluster
#[derive(Debug, Clone)]
pub struct InterestTracker {
    /// Map of subject patterns to interested nodes
    /// Pattern -> Set of NodeIds
    pattern_interests: Arc<RwLock<HashMap<String, HashSet<NodeId>>>>,

    /// Reverse index: NodeId -> Set of patterns
    node_patterns: Arc<RwLock<HashMap<NodeId, HashSet<String>>>>,

    /// Local subscriptions: subscription_id -> pattern
    local_subscriptions: Arc<RwLock<HashMap<String, String>>>,
}

impl InterestTracker {
    /// Create a new interest tracker
    pub fn new() -> Self {
        Self {
            pattern_interests: Arc::new(RwLock::new(HashMap::new())),
            node_patterns: Arc::new(RwLock::new(HashMap::new())),
            local_subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a remote node's interest in a subject pattern
    pub fn add_remote_interest(&self, node_id: NodeId, pattern: &str) -> PubSubResult<()> {
        validate_subject_pattern(pattern)
            .map_err(|e| PubSubError::InvalidSubject(e.to_string()))?;

        let mut pattern_interests = self.pattern_interests.write();
        let mut node_patterns = self.node_patterns.write();

        // Add to pattern -> nodes mapping
        pattern_interests
            .entry(pattern.to_string())
            .or_default()
            .insert(node_id.clone());

        // Add to node -> patterns mapping
        node_patterns
            .entry(node_id.clone())
            .or_default()
            .insert(pattern.to_string());

        debug!("Added remote interest: {} -> {}", node_id, pattern);
        Ok(())
    }

    /// Remove a remote node's interest in a subject pattern
    pub fn remove_remote_interest(&self, node_id: &NodeId, pattern: &str) -> PubSubResult<()> {
        let mut pattern_interests = self.pattern_interests.write();
        let mut node_patterns = self.node_patterns.write();

        // Remove from pattern -> nodes mapping
        if let Some(nodes) = pattern_interests.get_mut(pattern) {
            nodes.remove(node_id);
            if nodes.is_empty() {
                pattern_interests.remove(pattern);
            }
        }

        // Remove from node -> patterns mapping
        if let Some(patterns) = node_patterns.get_mut(node_id) {
            patterns.remove(pattern);
            if patterns.is_empty() {
                node_patterns.remove(node_id);
            }
        }

        debug!("Removed remote interest: {} -> {}", node_id, pattern);
        Ok(())
    }

    /// Update all interests for a node (replace existing)
    pub fn update_node_interests(
        &self,
        node_id: NodeId,
        interests: HashSet<String>,
    ) -> PubSubResult<()> {
        // Validate all patterns first
        for pattern in &interests {
            validate_subject_pattern(pattern)
                .map_err(|e| PubSubError::InvalidSubject(e.to_string()))?;
        }

        let mut pattern_interests = self.pattern_interests.write();
        let mut node_patterns = self.node_patterns.write();

        // Remove old interests
        if let Some(old_patterns) = node_patterns.remove(&node_id) {
            for pattern in old_patterns {
                if let Some(nodes) = pattern_interests.get_mut(&pattern) {
                    nodes.remove(&node_id);
                    if nodes.is_empty() {
                        pattern_interests.remove(&pattern);
                    }
                }
            }
        }

        // Add new interests
        if !interests.is_empty() {
            node_patterns.insert(node_id.clone(), interests.clone());

            for pattern in interests {
                pattern_interests
                    .entry(pattern)
                    .or_default()
                    .insert(node_id.clone());
            }
        }

        debug!("Updated interests for node {}", node_id);
        Ok(())
    }

    /// Remove all interests for a disconnected node
    pub fn remove_node(&self, node_id: &NodeId) {
        let mut pattern_interests = self.pattern_interests.write();
        let mut node_patterns = self.node_patterns.write();

        if let Some(patterns) = node_patterns.remove(node_id) {
            for pattern in patterns {
                if let Some(nodes) = pattern_interests.get_mut(&pattern) {
                    nodes.remove(node_id);
                    if nodes.is_empty() {
                        pattern_interests.remove(&pattern);
                    }
                }
            }
        }

        debug!("Removed all interests for disconnected node {}", node_id);
    }

    /// Find all nodes interested in a specific subject
    pub fn find_interested_nodes(&self, subject: &str) -> HashSet<NodeId> {
        let pattern_interests = self.pattern_interests.read();
        let mut interested_nodes = HashSet::new();

        // Check each pattern to see if it matches the subject
        for (pattern, nodes) in pattern_interests.iter() {
            if subject_matches_pattern(subject, pattern) {
                interested_nodes.extend(nodes.iter().cloned());
            }
        }

        trace!(
            "Found {} interested nodes for subject {}",
            interested_nodes.len(),
            subject
        );
        interested_nodes
    }

    /// Get all patterns a specific node is interested in
    #[allow(dead_code)]
    pub fn get_node_interests(&self, node_id: &NodeId) -> HashSet<String> {
        let node_patterns = self.node_patterns.read();
        node_patterns.get(node_id).cloned().unwrap_or_default()
    }

    /// Add a local subscription
    pub fn add_local_subscription(
        &self,
        subscription_id: String,
        pattern: String,
    ) -> PubSubResult<()> {
        validate_subject_pattern(&pattern)
            .map_err(|e| PubSubError::InvalidSubject(e.to_string()))?;

        let mut local_subs = self.local_subscriptions.write();
        local_subs.insert(subscription_id, pattern);
        Ok(())
    }

    /// Remove a local subscription
    pub fn remove_local_subscription(&self, subscription_id: &str) -> Option<String> {
        let mut local_subs = self.local_subscriptions.write();
        local_subs.remove(subscription_id)
    }

    /// Get all local subscription patterns
    pub fn get_local_patterns(&self) -> HashSet<String> {
        let local_subs = self.local_subscriptions.read();
        local_subs.values().cloned().collect()
    }

    /// Get subscription IDs that are interested in a specific subject
    pub fn get_local_subscriptions_for_subject(&self, subject: &str) -> Vec<String> {
        let local_subs = self.local_subscriptions.read();
        local_subs
            .iter()
            .filter(|(_, pattern)| subject_matches_pattern(subject, pattern))
            .map(|(sub_id, _)| sub_id.clone())
            .collect()
    }

    /// Get all remote nodes that have any interests
    pub fn get_all_interested_nodes(&self) -> HashSet<NodeId> {
        let node_patterns = self.node_patterns.read();
        node_patterns.keys().cloned().collect()
    }

    /// Clear all interest data
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.pattern_interests.write().clear();
        self.node_patterns.write().clear();
        self.local_subscriptions.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test NodeId from a seed value
    fn test_node_id(seed: u8) -> NodeId {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        NodeId::new(verifying_key)
    }

    #[test]
    fn test_interest_tracker() {
        let tracker = InterestTracker::new();
        let node1 = test_node_id(1);
        let node2 = test_node_id(2);

        // Add interests
        tracker.add_remote_interest(node1.clone(), "foo.*").unwrap();
        tracker.add_remote_interest(node1.clone(), "bar.>").unwrap();
        tracker
            .add_remote_interest(node2.clone(), "foo.bar")
            .unwrap();

        // Find interested nodes
        let interested = tracker.find_interested_nodes("foo.bar");
        assert_eq!(interested.len(), 2);
        assert!(interested.contains(&node1));
        assert!(interested.contains(&node2));

        let interested = tracker.find_interested_nodes("bar.baz.qux");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node1));

        // Remove interest
        tracker.remove_remote_interest(&node1, "foo.*").unwrap();
        let interested = tracker.find_interested_nodes("foo.test");
        assert_eq!(interested.len(), 0);

        // Remove node
        tracker.remove_node(&node2);
        let interested = tracker.find_interested_nodes("foo.bar");
        assert_eq!(interested.len(), 0); // node1 no longer has foo.*, and node2 is removed

        // But node1 should still be interested in bar.> patterns
        let interested = tracker.find_interested_nodes("bar.test");
        assert_eq!(interested.len(), 1);
        assert!(interested.contains(&node1));
    }

    #[test]
    fn test_update_node_interests() {
        let tracker = InterestTracker::new();
        let node = test_node_id(1);

        // Initial interests
        let interests1: HashSet<_> = ["foo.*", "bar.>"].iter().map(|s| s.to_string()).collect();
        tracker
            .update_node_interests(node.clone(), interests1)
            .unwrap();

        assert_eq!(tracker.get_node_interests(&node).len(), 2);

        // Update with new interests
        let interests2: HashSet<_> = ["baz.*", "qux.>"].iter().map(|s| s.to_string()).collect();
        tracker
            .update_node_interests(node.clone(), interests2)
            .unwrap();

        assert_eq!(tracker.get_node_interests(&node).len(), 2);
        assert!(!tracker.find_interested_nodes("foo.test").contains(&node));
        assert!(tracker.find_interested_nodes("baz.test").contains(&node));
    }
}
