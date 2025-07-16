//! System views for monitoring - provides read-only access to system state

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;

use super::types::*;
use crate::{error::ConsensusResult, foundation::ConsensusGroupId};
use proven_topology::NodeId;

/// Cluster information
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Number of nodes in the cluster
    pub node_count: usize,
    /// Current global leader
    pub global_leader: Option<NodeId>,
    /// Cluster formation time
    pub formed_at: Option<SystemTime>,
}

/// System view provides read-only access to consensus state
///
/// This is a simplified version that doesn't depend on Transport/Governance generics
pub struct SystemView {
    /// Cached cluster info
    cluster_info: Arc<RwLock<Option<ClusterInfo>>>,

    /// Cached stream health data
    stream_health: Arc<RwLock<HashMap<String, StreamHealth>>>,

    /// Cached group health data
    group_health: Arc<RwLock<HashMap<ConsensusGroupId, GroupHealth>>>,

    /// Cached node health data
    node_health: Arc<RwLock<HashMap<NodeId, NodeHealth>>>,

    /// Last refresh time
    last_refresh: Arc<RwLock<SystemTime>>,
}

impl SystemView {
    /// Create a new system view
    pub fn new() -> Self {
        Self {
            cluster_info: Arc::new(RwLock::new(None)),
            stream_health: Arc::new(RwLock::new(HashMap::new())),
            group_health: Arc::new(RwLock::new(HashMap::new())),
            node_health: Arc::new(RwLock::new(HashMap::new())),
            last_refresh: Arc::new(RwLock::new(SystemTime::UNIX_EPOCH)),
        }
    }

    /// Refresh all cached data
    pub async fn refresh(&self) -> ConsensusResult<()> {
        // In a real implementation, this would fetch data from the consensus engine
        // For now, we'll just update the refresh time
        let mut last_refresh = self.last_refresh.write().await;
        *last_refresh = SystemTime::now();

        Ok(())
    }

    /// Get cluster information
    pub async fn get_cluster_info(&self) -> ConsensusResult<ClusterInfo> {
        let info = self.cluster_info.read().await;
        Ok(info.clone().unwrap_or(ClusterInfo {
            node_count: 0,
            global_leader: None,
            formed_at: None,
        }))
    }

    /// Check if there's a global leader
    pub async fn has_global_leader(&self) -> ConsensusResult<bool> {
        let info = self.get_cluster_info().await?;
        Ok(info.global_leader.is_some())
    }

    /// Check if global consensus is stable
    pub async fn is_global_consensus_stable(&self) -> ConsensusResult<bool> {
        // In a real implementation, we'd check term stability, etc.
        Ok(true)
    }

    /// Get connected peer count
    pub async fn get_connected_peer_count(&self) -> ConsensusResult<usize> {
        let nodes = self.node_health.read().await;
        Ok(nodes
            .values()
            .filter(|n| n.status != HealthStatus::Unknown)
            .count())
    }

    /// Get expected peer count
    pub async fn get_expected_peer_count(&self) -> ConsensusResult<usize> {
        let info = self.get_cluster_info().await?;
        Ok(info.node_count.saturating_sub(1)) // Exclude self
    }

    /// Get group count
    pub async fn get_group_count(&self) -> ConsensusResult<usize> {
        let groups = self.group_health.read().await;
        Ok(groups.len())
    }

    /// Get stream count
    pub async fn get_stream_count(&self) -> ConsensusResult<usize> {
        let streams = self.stream_health.read().await;
        Ok(streams.len())
    }

    /// Get stream health
    pub async fn get_stream_health(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<StreamHealth>> {
        let streams = self.stream_health.read().await;
        Ok(streams.get(stream_name).cloned())
    }

    /// Get all stream health data
    pub async fn get_all_stream_health(&self) -> ConsensusResult<Vec<StreamHealth>> {
        let streams = self.stream_health.read().await;
        Ok(streams.values().cloned().collect())
    }

    /// Get group health
    pub async fn get_group_health(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupHealth>> {
        let groups = self.group_health.read().await;
        Ok(groups.get(&group_id).cloned())
    }

    /// Get all group health data
    pub async fn get_all_group_health(&self) -> ConsensusResult<Vec<GroupHealth>> {
        let groups = self.group_health.read().await;
        Ok(groups.values().cloned().collect())
    }

    /// Get node health
    pub async fn get_node_health(&self, node_id: &NodeId) -> ConsensusResult<Option<NodeHealth>> {
        let nodes = self.node_health.read().await;
        Ok(nodes.get(node_id).cloned())
    }

    /// Get all node health data
    pub async fn get_all_node_health(&self) -> ConsensusResult<Vec<NodeHealth>> {
        let nodes = self.node_health.read().await;
        Ok(nodes.values().cloned().collect())
    }

    /// Update cluster info (for testing/external updates)
    pub async fn update_cluster_info(&self, info: ClusterInfo) -> ConsensusResult<()> {
        let mut cluster_info = self.cluster_info.write().await;
        *cluster_info = Some(info);
        Ok(())
    }

    /// Update stream health (for testing/external updates)
    pub async fn update_stream_health(
        &self,
        stream_name: String,
        health: StreamHealth,
    ) -> ConsensusResult<()> {
        let mut streams = self.stream_health.write().await;
        streams.insert(stream_name, health);
        Ok(())
    }

    /// Update group health (for testing/external updates)
    pub async fn update_group_health(
        &self,
        group_id: ConsensusGroupId,
        health: GroupHealth,
    ) -> ConsensusResult<()> {
        let mut groups = self.group_health.write().await;
        groups.insert(group_id, health);
        Ok(())
    }

    /// Update node health (for testing/external updates)
    pub async fn update_node_health(
        &self,
        node_id: NodeId,
        health: NodeHealth,
    ) -> ConsensusResult<()> {
        let mut nodes = self.node_health.write().await;
        nodes.insert(node_id, health);
        Ok(())
    }
}
