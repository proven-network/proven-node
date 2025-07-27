//! Global consensus state
//!
//! Pure state container for global consensus operations.
//! This module only manages state - all validation happens elsewhere.

use std::sync::Arc;

use dashmap::DashMap;

use crate::foundation::StreamConfig;
use crate::foundation::models::{GroupInfo, NodeInfo, StreamInfo};
use crate::foundation::types::{ConsensusGroupId, StreamName};
use proven_topology::NodeId;

/// Global consensus state
#[derive(Clone)]
pub struct GlobalState {
    /// Stream configurations
    streams: Arc<DashMap<StreamName, StreamInfo>>,

    /// Consensus groups
    groups: Arc<DashMap<ConsensusGroupId, GroupInfo>>,

    /// Node to groups mapping
    node_groups: Arc<DashMap<NodeId, Vec<ConsensusGroupId>>>,

    /// Cluster members
    members: Arc<DashMap<NodeId, NodeInfo>>,
}

impl GlobalState {
    /// Create new global state
    pub fn new() -> Self {
        Self {
            streams: Arc::new(DashMap::new()),
            groups: Arc::new(DashMap::new()),
            node_groups: Arc::new(DashMap::new()),
            members: Arc::new(DashMap::new()),
        }
    }

    /// Clear all state (used when installing snapshots)
    pub async fn clear(&self) {
        self.streams.clear();
        self.groups.clear();
        self.node_groups.clear();
        self.members.clear();
    }

    // Stream operations

    /// Add a stream
    pub async fn add_stream(&self, info: StreamInfo) -> crate::error::ConsensusResult<()> {
        self.streams.insert(info.name.clone(), info);
        Ok(())
    }

    /// Remove a stream
    pub async fn remove_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        self.streams.remove(name).map(|(_, v)| v)
    }

    /// Get stream info
    pub async fn get_stream(&self, name: &StreamName) -> Option<StreamInfo> {
        self.streams.get(name).map(|entry| entry.clone())
    }

    /// List all streams
    pub async fn list_streams(&self) -> Vec<StreamInfo> {
        self.streams
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all streams (alias for list_streams for consistency)
    pub async fn get_all_streams(&self) -> Vec<StreamInfo> {
        self.list_streams().await
    }

    /// Update stream config
    pub async fn update_stream_config(&self, name: &StreamName, config: StreamConfig) -> bool {
        if let Some(mut entry) = self.streams.get_mut(name) {
            entry.config = config;
            true
        } else {
            false
        }
    }

    /// Reassign stream to different group
    pub async fn reassign_stream(&self, name: &StreamName, group_id: ConsensusGroupId) -> bool {
        if let Some(mut entry) = self.streams.get_mut(name) {
            entry.group_id = group_id;
            true
        } else {
            false
        }
    }

    // Group operations

    /// Add a consensus group
    pub async fn add_group(&self, info: GroupInfo) -> crate::error::ConsensusResult<()> {
        self.groups.insert(info.id, info.clone());

        // Update node mappings
        for member in &info.members {
            self.node_groups
                .entry(member.clone())
                .or_default()
                .push(info.id);
        }
        Ok(())
    }

    /// Remove a consensus group
    pub async fn remove_group(&self, id: ConsensusGroupId) -> Option<GroupInfo> {
        if let Some((_, info)) = self.groups.remove(&id) {
            // Update node mappings
            for member in &info.members {
                if let Some(mut groups) = self.node_groups.get_mut(member) {
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
        self.groups.get(id).map(|entry| entry.clone())
    }

    /// List all groups
    pub async fn list_groups(&self) -> Vec<GroupInfo> {
        self.groups
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all groups (alias for list_groups)
    pub async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.list_groups().await
    }

    /// Add member to group
    pub async fn add_group_member(&self, group_id: ConsensusGroupId, node_id: NodeId) -> bool {
        if let Some(mut info) = self.groups.get_mut(&group_id) {
            if !info.members.contains(&node_id) {
                info.members.push(node_id.clone());

                // Update node mappings
                self.node_groups.entry(node_id).or_default().push(group_id);
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
        if let Some(mut info) = self.groups.get_mut(&group_id) {
            let before = info.members.len();
            info.members.retain(|n| n != node_id);
            let removed = before != info.members.len();

            if removed {
                // Update node mappings
                if let Some(mut groups) = self.node_groups.get_mut(node_id) {
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
        self.members.insert(info.node_id.clone(), info);
    }

    /// Remove cluster member
    pub async fn remove_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.members.remove(node_id).map(|(_, v)| v)
    }

    /// Get member info
    pub async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.members.get(node_id).map(|entry| entry.clone())
    }

    /// List all members
    pub async fn list_members(&self) -> Vec<NodeInfo> {
        self.members
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get groups for a node
    pub async fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        self.node_groups
            .get(node_id)
            .map(|entry| entry.clone())
            .unwrap_or_default()
    }

    // Query operations

    /// Count streams in a group
    pub async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> usize {
        self.streams
            .iter()
            .filter(|entry| entry.value().group_id == group_id)
            .count()
    }

    /// Get streams for a group
    pub async fn get_streams_for_group(&self, group_id: ConsensusGroupId) -> Vec<StreamInfo> {
        self.streams
            .iter()
            .filter(|entry| entry.value().group_id == group_id)
            .map(|entry| entry.value().clone())
            .collect()
    }
}

impl Default for GlobalState {
    fn default() -> Self {
        Self::new()
    }
}
