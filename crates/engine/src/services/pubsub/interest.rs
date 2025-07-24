//! Interest tracking for distributed PubSub

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::foundation::types::{SubjectPattern, subject_matches_pattern};
use proven_topology::NodeId;

/// Tracks which nodes are interested in which subjects
#[derive(Clone)]
pub struct InterestTracker {
    /// Node interests: node_id -> set of subject patterns
    node_interests: Arc<RwLock<HashMap<NodeId, HashSet<SubjectPattern>>>>,
    /// Reverse index: subject pattern -> set of interested nodes
    pattern_nodes: Arc<RwLock<HashMap<SubjectPattern, HashSet<NodeId>>>>,
}

impl InterestTracker {
    /// Create a new interest tracker
    pub fn new() -> Self {
        Self {
            node_interests: Arc::new(RwLock::new(HashMap::new())),
            pattern_nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update a node's interest set
    pub async fn update_interests(&self, node_id: NodeId, patterns: Vec<SubjectPattern>) {
        let mut node_interests = self.node_interests.write().await;
        let mut pattern_nodes = self.pattern_nodes.write().await;

        // Remove old interests
        if let Some(old_patterns) = node_interests.get(&node_id) {
            for pattern in old_patterns {
                if let Some(nodes) = pattern_nodes.get_mut(pattern) {
                    nodes.remove(&node_id);
                    if nodes.is_empty() {
                        pattern_nodes.remove(pattern);
                    }
                }
            }
        }

        // Add new interests
        let pattern_set: HashSet<SubjectPattern> = patterns.into_iter().collect();
        for pattern in &pattern_set {
            pattern_nodes
                .entry(pattern.clone())
                .or_default()
                .insert(node_id.clone());
        }

        node_interests.insert(node_id, pattern_set);
    }

    /// Remove all interests for a node
    pub async fn remove_node(&self, node_id: &NodeId) {
        let mut node_interests = self.node_interests.write().await;
        let mut pattern_nodes = self.pattern_nodes.write().await;

        if let Some(patterns) = node_interests.remove(node_id) {
            for pattern in patterns {
                if let Some(nodes) = pattern_nodes.get_mut(&pattern) {
                    nodes.remove(node_id);
                    if nodes.is_empty() {
                        pattern_nodes.remove(&pattern);
                    }
                }
            }
        }
    }

    /// Add a single interest for a node
    pub async fn add_interest(&self, node_id: NodeId, pattern: SubjectPattern) {
        let mut node_interests = self.node_interests.write().await;
        let mut pattern_nodes = self.pattern_nodes.write().await;

        // Add to node's interests
        node_interests
            .entry(node_id.clone())
            .or_default()
            .insert(pattern.clone());

        // Add to pattern index
        pattern_nodes.entry(pattern).or_default().insert(node_id);
    }

    /// Remove a single interest for a node
    pub async fn remove_interest(&self, node_id: NodeId, pattern: &SubjectPattern) {
        let mut node_interests = self.node_interests.write().await;
        let mut pattern_nodes = self.pattern_nodes.write().await;

        // Remove from node's interests
        if let Some(interests) = node_interests.get_mut(&node_id) {
            interests.remove(pattern);
            if interests.is_empty() {
                node_interests.remove(&node_id);
            }
        }

        // Remove from pattern index
        if let Some(nodes) = pattern_nodes.get_mut(pattern) {
            nodes.remove(&node_id);
            if nodes.is_empty() {
                pattern_nodes.remove(pattern);
            }
        }
    }

    /// Find nodes interested in a specific subject
    pub async fn find_interested_nodes(&self, subject: &str) -> HashSet<NodeId> {
        let pattern_nodes = self.pattern_nodes.read().await;
        let mut interested = HashSet::new();

        for (pattern, nodes) in pattern_nodes.iter() {
            if subject_matches_pattern(subject, pattern.as_ref()) {
                interested.extend(nodes.iter().cloned());
            }
        }

        interested
    }

    /// Get all interests for a specific node
    pub async fn get_node_interests(&self, node_id: &NodeId) -> Option<HashSet<SubjectPattern>> {
        self.node_interests.read().await.get(node_id).cloned()
    }

    /// Get all known interests
    pub async fn get_all_interests(&self) -> HashMap<NodeId, HashSet<SubjectPattern>> {
        self.node_interests.read().await.clone()
    }

    /// Get all nodes that have registered interests
    pub async fn get_all_nodes(&self) -> Vec<NodeId> {
        self.node_interests.read().await.keys().cloned().collect()
    }

    /// Check if any node is interested in a subject
    pub async fn has_interest(&self, subject: &str) -> bool {
        let pattern_nodes = self.pattern_nodes.read().await;

        for pattern in pattern_nodes.keys() {
            if subject_matches_pattern(subject, pattern.as_ref()) {
                return true;
            }
        }

        false
    }

    /// Clear all interests
    pub async fn clear(&self) {
        self.node_interests.write().await.clear();
        self.pattern_nodes.write().await.clear();
    }
}

impl Default for InterestTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_interest_tracking() {
        let tracker = InterestTracker::new();
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        // Add interests
        tracker
            .update_interests(
                node1.clone(),
                vec![
                    SubjectPattern::new("metrics.*".to_string()).unwrap(),
                    SubjectPattern::new("events.>".to_string()).unwrap(),
                ],
            )
            .await;

        tracker
            .update_interests(
                node2.clone(),
                vec![
                    SubjectPattern::new("metrics.cpu".to_string()).unwrap(),
                    SubjectPattern::new("logs.>".to_string()).unwrap(),
                ],
            )
            .await;

        // Test finding interested nodes
        let interested = tracker.find_interested_nodes("metrics.cpu").await;
        assert!(interested.contains(&node1));
        assert!(interested.contains(&node2));

        let interested = tracker.find_interested_nodes("events.user.login").await;
        assert!(interested.contains(&node1));
        assert!(!interested.contains(&node2));

        // Test has_interest
        assert!(tracker.has_interest("metrics.memory").await);
        assert!(!tracker.has_interest("unknown.subject").await);

        // Remove node
        tracker.remove_node(&node1).await;
        let interested = tracker.find_interested_nodes("metrics.memory").await;
        assert!(!interested.contains(&node1));
    }

    #[tokio::test]
    async fn test_interest_updates() {
        let tracker = InterestTracker::new();
        let node = NodeId::from_seed(1);

        // Initial interests
        tracker
            .update_interests(
                node.clone(),
                vec![
                    SubjectPattern::new("foo.*".to_string()).unwrap(),
                    SubjectPattern::new("bar.>".to_string()).unwrap(),
                ],
            )
            .await;

        let interests = tracker.get_node_interests(&node).await.unwrap();
        assert_eq!(interests.len(), 2);

        // Update interests
        tracker
            .update_interests(
                node.clone(),
                vec![SubjectPattern::new("baz.*".to_string()).unwrap()],
            )
            .await;

        let interests = tracker.get_node_interests(&node).await.unwrap();
        assert_eq!(interests.len(), 1);
        let baz_pattern = SubjectPattern::new("baz.*".to_string()).unwrap();
        assert!(interests.contains(&baz_pattern));
        let foo_pattern = SubjectPattern::new("foo.*".to_string()).unwrap();
        assert!(!interests.contains(&foo_pattern));
    }
}
