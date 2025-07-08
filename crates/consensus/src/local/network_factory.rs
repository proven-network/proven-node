//! Network factory implementation for local consensus groups
//!
//! This module provides network factories for local consensus groups,
//! reusing the generic network/adaptor.rs infrastructure.

use super::LocalTypeConfig;
use crate::allocation::ConsensusGroupId;
use crate::network::adaptor::{
    NetworkFactory as BaseNetworkFactory, RaftAdapterRequest, RaftAdapterResponse,
};
use crate::node_id::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

/// Message channels for a local consensus group
pub struct GroupChannels {
    /// Sender for outgoing requests
    pub request_tx:
        mpsc::UnboundedSender<(NodeId, uuid::Uuid, Box<RaftAdapterRequest<LocalTypeConfig>>)>,
    /// Receiver for incoming responses  
    pub response_rx: mpsc::UnboundedReceiver<(
        NodeId,
        uuid::Uuid,
        Box<RaftAdapterResponse<LocalTypeConfig>>,
    )>,
}

/// Registry for managing network infrastructure across local consensus groups
pub struct LocalNetworkRegistry {
    /// Map of group IDs to their message channels
    group_channels: Arc<RwLock<HashMap<ConsensusGroupId, GroupChannels>>>,
    /// Global map of node to group assignments
    node_groups: Arc<RwLock<HashMap<NodeId, ConsensusGroupId>>>,
}

impl Default for LocalNetworkRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalNetworkRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            group_channels: Arc::new(RwLock::new(HashMap::new())),
            node_groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a network factory for a local consensus group
    pub async fn create_network_factory(
        &self,
        group_id: ConsensusGroupId,
    ) -> BaseNetworkFactory<LocalTypeConfig> {
        // Create channels for this group if they don't exist
        let mut channels = self.group_channels.write().await;

        if let Some(group_channels) = channels.remove(&group_id) {
            // Take ownership of the channels to create the factory
            BaseNetworkFactory::new(group_channels.request_tx, group_channels.response_rx)
        } else {
            // Create new channels
            let (req_tx, _req_rx) = mpsc::unbounded_channel();
            let (_resp_tx, resp_rx) = mpsc::unbounded_channel();

            // Create the network factory using the generic implementation
            BaseNetworkFactory::new(req_tx, resp_rx)
        }
    }

    /// Store channels for a group (for later retrieval)
    pub async fn store_group_channels(
        &self,
        group_id: ConsensusGroupId,
        request_tx: mpsc::UnboundedSender<(
            NodeId,
            uuid::Uuid,
            Box<RaftAdapterRequest<LocalTypeConfig>>,
        )>,
        response_rx: mpsc::UnboundedReceiver<(
            NodeId,
            uuid::Uuid,
            Box<RaftAdapterResponse<LocalTypeConfig>>,
        )>,
    ) {
        let mut channels = self.group_channels.write().await;
        channels.insert(
            group_id,
            GroupChannels {
                request_tx,
                response_rx,
            },
        );
    }

    /// Update node group assignment
    pub async fn assign_node_to_group(&self, node_id: NodeId, group_id: ConsensusGroupId) {
        let mut groups = self.node_groups.write().await;
        groups.insert(node_id, group_id);
    }

    /// Remove node from group assignment
    pub async fn remove_node_from_group(&self, node_id: &NodeId) {
        let mut groups = self.node_groups.write().await;
        groups.remove(node_id);
    }

    /// Get all nodes in a specific group
    pub async fn get_group_members(&self, group_id: ConsensusGroupId) -> Vec<NodeId> {
        let groups = self.node_groups.read().await;
        groups
            .iter()
            .filter(|(_, gid)| **gid == group_id)
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }

    /// Check if a node belongs to a specific group
    pub async fn is_node_in_group(&self, node_id: &NodeId, group_id: ConsensusGroupId) -> bool {
        let groups = self.node_groups.read().await;
        groups
            .get(node_id)
            .map(|gid| *gid == group_id)
            .unwrap_or(false)
    }
}
