//! Interest management interface for PubSub service

use crate::foundation::Message;
use crate::foundation::types::SubjectPattern;
use async_trait::async_trait;
use proven_topology::NodeId;

/// Interface for managing interests and routing messages
#[async_trait]
pub trait InterestManager: Send + Sync {
    /// Add interest pattern for a node
    async fn add_node_interest(&self, node_id: NodeId, pattern: SubjectPattern);

    /// Remove interest pattern for a node
    async fn remove_node_interest(&self, node_id: NodeId, pattern: &SubjectPattern);

    /// Remove all interests for a node
    async fn remove_all_node_interests(&self, node_id: &NodeId);

    /// Update peer interests (replace all)
    async fn update_peer_interests(&self, node_id: NodeId, patterns: Vec<SubjectPattern>);

    /// Route a message to local subscribers
    async fn route_message(&self, message: &Message) -> usize;
}
