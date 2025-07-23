//! Global consensus state
//!
//! Pure state container for global consensus operations.
//! This module only manages state - all validation happens elsewhere.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::{
    foundation::types::ConsensusGroupId,
    services::stream::{StreamConfig, StreamName},
};
use proven_topology::NodeId;

use super::types::GroupInfo;

/// Global consensus state
#[derive(Clone)]
pub struct GlobalState {
    /// Stream configurations
    streams: Arc<RwLock<HashMap<StreamName, StreamInfo>>>,

    /// Consensus groups
    groups: Arc<RwLock<HashMap<ConsensusGroupId, GroupInfo>>>,

    /// Node to groups mapping
    node_groups: Arc<RwLock<HashMap<NodeId, Vec<ConsensusGroupId>>>>,

    /// Cluster members
    members: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
}

/// Stream information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    /// Stream name
    pub name: StreamName,
    /// Stream configuration
    pub config: StreamConfig,
    /// Assigned consensus group
    pub group_id: ConsensusGroupId,
    /// Creation timestamp
    pub created_at: u64,
}

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub node_id: NodeId,
    /// When the node joined
    pub joined_at: u64,
    /// Node metadata
    pub metadata: HashMap<String, String>,
}

impl GlobalState {
    /// Create new global state
    pub fn new() -> Self {
        Self {
            streams: Arc::new(RwLock::new(HashMap::new())),
            groups: Arc::new(RwLock::new(HashMap::new())),
            node_groups: Arc::new(RwLock::new(HashMap::new())),
            members: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Clear all state (used when installing snapshots)
    pub async fn clear(&self) {
        self.streams.write().await.clear();
        self.groups.write().await.clear();
        self.node_groups.write().await.clear();
        self.members.write().await.clear();
    }

    // Stream operations

    /// Add a stream
    pub async fn add_stream(&self, info: StreamInfo) -> crate::error::ConsensusResult<()> {
        let mut streams = self.streams.write().await;
        streams.insert(info.name.clone(), info);
        Ok(())
    }

    /// Remove a stream
    pub async fn remove_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        let mut streams = self.streams.write().await;
        streams.remove(name)
    }

    /// Get stream info
    pub async fn get_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        let streams = self.streams.read().await;
        streams.get(name).cloned()
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamInfo> {
        let streams = self.streams.read().await;
        streams.values().cloned().collect()
    }

    /// Get all streams (alias for list_streams for consistency)
    pub async fn get_all_streams(&self) -> Vec<StreamInfo> {
        self.list_streams().await
    }

    /// Update stream config
    pub async fn update_stream_config(&self, name: &StreamName, config: StreamConfig) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(info) = streams.get_mut(name) {
            info.config = config;
            true
        } else {
            false
        }
    }

    /// Reassign stream to different group
    pub async fn reassign_stream(&self, name: &StreamName, group_id: ConsensusGroupId) -> bool {
        let mut streams = self.streams.write().await;
        if let Some(info) = streams.get_mut(name) {
            info.group_id = group_id;
            true
        } else {
            false
        }
    }

    // Group operations

    /// Add a consensus group
    pub async fn add_group(&self, info: GroupInfo) -> crate::error::ConsensusResult<()> {
        let mut groups = self.groups.write().await;
        groups.insert(info.id, info.clone());

        // Update node mappings
        let mut node_groups = self.node_groups.write().await;
        for member in &info.members {
            node_groups
                .entry(member.clone())
                .or_insert_with(Vec::new)
                .push(info.id);
        }
        Ok(())
    }

    /// Remove a consensus group
    pub async fn remove_group(&self, id: ConsensusGroupId) -> Option<GroupInfo> {
        let mut groups = self.groups.write().await;
        if let Some(info) = groups.remove(&id) {
            // Update node mappings
            let mut node_groups = self.node_groups.write().await;
            for member in &info.members {
                if let Some(groups) = node_groups.get_mut(member) {
                    groups.retain(|&g| g != id);
                }
            }
            Some(info)
        } else {
            None
        }
    }

    /// Get group info
    pub async fn get_group(&self, id: &ConsensusGroupId) -> Option<GroupInfo> {
        let groups = self.groups.read().await;
        groups.get(id).cloned()
    }

    /// List all groups
    pub async fn list_groups(&self) -> Vec<GroupInfo> {
        let groups = self.groups.read().await;
        groups.values().cloned().collect()
    }

    /// Get all groups (alias for list_groups)
    pub async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.list_groups().await
    }

    /// Add member to group
    pub async fn add_group_member(&self, group_id: ConsensusGroupId, node_id: NodeId) -> bool {
        let mut groups = self.groups.write().await;
        if let Some(info) = groups.get_mut(&group_id) {
            if !info.members.contains(&node_id) {
                info.members.push(node_id.clone());

                // Update node mappings
                drop(groups);
                let mut node_groups = self.node_groups.write().await;
                node_groups
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(group_id);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Remove member from group
    pub async fn remove_group_member(&self, group_id: ConsensusGroupId, node_id: &NodeId) -> bool {
        let mut groups = self.groups.write().await;
        if let Some(info) = groups.get_mut(&group_id) {
            let before = info.members.len();
            info.members.retain(|n| n != node_id);
            let removed = before != info.members.len();

            if removed {
                // Update node mappings
                drop(groups);
                let mut node_groups = self.node_groups.write().await;
                if let Some(groups) = node_groups.get_mut(node_id) {
                    groups.retain(|&g| g != group_id);
                }
            }
            removed
        } else {
            false
        }
    }

    // Node operations

    /// Add cluster member
    pub async fn add_member(&self, info: NodeInfo) {
        let mut members = self.members.write().await;
        members.insert(info.node_id.clone(), info);
    }

    /// Remove cluster member
    pub async fn remove_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let mut members = self.members.write().await;
        members.remove(node_id)
    }

    /// Get member info
    pub async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        let members = self.members.read().await;
        members.get(node_id).cloned()
    }

    /// List all members
    pub async fn list_members(&self) -> Vec<NodeInfo> {
        let members = self.members.read().await;
        members.values().cloned().collect()
    }

    /// Get groups for a node
    pub async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        let node_groups = self.node_groups.read().await;
        node_groups.get(node_id).cloned().unwrap_or_default()
    }

    // Query operations

    /// Count streams in a group
    pub async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize {
        let streams = self.streams.read().await;
        streams.values().filter(|s| s.group_id == group_id).count()
    }

    /// Get streams for a group
    pub async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo> {
        let streams = self.streams.read().await;
        streams
            .values()
            .filter(|s| s.group_id == group_id)
            .cloned()
            .collect()
    }
}

impl Default for GlobalState {
    fn default() -> Self {
        Self::new()
    }
}
