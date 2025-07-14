//! Read-only views for monitoring and decision-making
//!
//! This module provides read-only views of system state that can be used
//! for monitoring, metrics collection, and orchestration decisions.

use crate::ConsensusGroupId;
use crate::core::global::GlobalState;
use crate::core::group::GroupsManager;
use crate::error::ConsensusResult;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use proven_governance::Governance;
use proven_topology::NodeId;
use proven_topology::TopologyManager;
use proven_transport::Transport;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Health status for monitored components
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but operational
    Degraded,
    /// Component is unhealthy
    Unhealthy,
    /// Component status is unknown
    Unknown,
}

/// Stream health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHealth {
    /// Stream name
    pub stream_name: String,
    /// Consensus group handling this stream
    pub group_id: ConsensusGroupId,
    /// Health status
    pub status: HealthStatus,
    /// Current message rate
    pub message_rate: f64,
    /// Storage size in bytes
    pub storage_bytes: u64,
    /// Last activity timestamp
    pub last_activity: Option<SystemTime>,
    /// Error count in last interval
    pub error_count: u32,
    /// Whether stream is paused
    pub is_paused: bool,
}

/// Group health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupHealth {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Health status
    pub status: HealthStatus,
    /// Number of members
    pub member_count: usize,
    /// Leader node (if any)
    pub leader: Option<NodeId>,
    /// Whether group has quorum
    pub has_quorum: bool,
    /// Number of streams in group
    pub stream_count: usize,
    /// Total storage used
    pub storage_bytes: u64,
    /// Group uptime
    pub uptime: Duration,
    /// Last election time
    pub last_election: Option<SystemTime>,
}

/// Node health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    /// Node ID
    pub node_id: NodeId,
    /// Health status
    pub status: HealthStatus,
    /// Number of groups this node participates in
    pub group_count: usize,
    /// CPU usage percentage
    pub cpu_usage: f32,
    /// Memory usage percentage
    pub memory_usage: f32,
    /// Storage usage percentage
    pub storage_usage: f32,
    /// Network latency to other nodes
    pub network_latency: HashMap<NodeId, Duration>,
    /// Whether node is reachable
    pub is_reachable: bool,
    /// Last seen timestamp
    pub last_seen: SystemTime,
}

/// Read-only view of stream state
#[derive(Clone)]
pub struct StreamView<T, G>
where
    T: Transport,
    G: Governance,
{
    global_state: Arc<GlobalState>,
    groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
}

impl<T, G> StreamView<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new stream view
    pub fn new(
        global_state: Arc<GlobalState>,
        groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
    ) -> Self {
        Self {
            global_state,
            groups_manager,
        }
    }

    /// Get health information for a specific stream
    pub async fn get_stream_health(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<StreamHealth>> {
        // Get stream info from global state
        let streams = self.global_state.streams.read().await;
        let stream_data = match streams.get(stream_name) {
            Some(data) => data,
            None => return Ok(None),
        };

        // Get stream config
        let stream_configs = self.global_state.stream_configs.read().await;
        let config = stream_configs.get(stream_name);

        // Get group_id from stream config
        let group_id = match config.and_then(|c| c.consensus_group) {
            Some(id) => id,
            None => return Ok(None), // Stream not assigned to a group
        };
        let has_group = self.groups_manager.read().await.has_group(group_id).await;

        let status = if !has_group {
            HealthStatus::Unhealthy
        } else if stream_data.subscriptions.is_empty() {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Ok(Some(StreamHealth {
            stream_name: stream_name.to_string(),
            group_id,
            status,
            message_rate: 0.0, // Would be calculated from metrics
            storage_bytes: 0,  // Would be calculated from storage
            last_activity: None,
            error_count: 0,
            is_paused: false, // TODO: Add paused state tracking if needed
        }))
    }

    /// Get health information for all streams
    pub async fn get_all_stream_health(&self) -> ConsensusResult<Vec<StreamHealth>> {
        let streams = self.global_state.streams.read().await;
        let mut health_info = Vec::new();

        for stream_name in streams.keys() {
            if let Some(health) = self.get_stream_health(stream_name).await? {
                health_info.push(health);
            }
        }

        Ok(health_info)
    }

    /// Get streams by health status
    pub async fn get_streams_by_status(
        &self,
        status: HealthStatus,
    ) -> ConsensusResult<Vec<String>> {
        let all_health = self.get_all_stream_health().await?;
        Ok(all_health
            .into_iter()
            .filter(|h| h.status == status)
            .map(|h| h.stream_name)
            .collect())
    }

    /// Get stream count by group
    pub async fn get_stream_count_by_group(
        &self,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, usize>> {
        let streams = self.global_state.streams.read().await;
        let mut counts = HashMap::new();

        let stream_configs = self.global_state.stream_configs.read().await;
        for (stream_name, _) in streams.iter() {
            if let Some(config) = stream_configs.get(stream_name)
                && let Some(group_id) = config.consensus_group
            {
                *counts.entry(group_id).or_insert(0) += 1;
            }
        }

        Ok(counts)
    }
}

/// Read-only view of group state
#[derive(Clone)]
pub struct GroupView<T, G>
where
    T: Transport,
    G: Governance,
{
    global_state: Arc<GlobalState>,
    groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
}

impl<T, G> GroupView<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new group view
    pub fn new(
        global_state: Arc<GlobalState>,
        groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
    ) -> Self {
        Self {
            global_state,
            groups_manager,
        }
    }

    /// Get health information for a specific group
    pub async fn get_group_health(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupHealth>> {
        // Check if group exists locally
        if !self.groups_manager.read().await.has_group(group_id).await {
            return Ok(None);
        }

        // Get group info from global state
        let groups = self.global_state.consensus_groups.read().await;
        let group_info = match groups.get(&group_id) {
            Some(info) => info,
            None => return Ok(None),
        };

        // Get metrics from the group
        let groups_manager = self.groups_manager.read().await;
        let is_leader = groups_manager.is_leader(group_id).await.unwrap_or(false);
        let leader = if is_leader {
            Some(groups_manager.node_id().clone())
        } else {
            None // ConsensusGroupInfo doesn't have a leader field
        };

        // Count streams in this group
        let stream_count = self.count_streams_in_group(group_id).await?;

        let status = if group_info.members.is_empty() {
            HealthStatus::Unhealthy
        } else if group_info.members.len() < 3 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Ok(Some(GroupHealth {
            group_id,
            status,
            member_count: group_info.members.len(),
            leader,
            has_quorum: group_info.members.len() >= 2, // Simple majority
            stream_count,
            storage_bytes: 0,               // Would be calculated from storage
            uptime: Duration::from_secs(0), // Would be tracked
            last_election: None,
        }))
    }

    /// Get health information for all groups
    pub async fn get_all_group_health(&self) -> ConsensusResult<Vec<GroupHealth>> {
        let group_ids = self.groups_manager.read().await.get_all_group_ids().await;
        let mut health_info = Vec::new();

        for group_id in group_ids {
            if let Some(health) = self.get_group_health(group_id).await? {
                health_info.push(health);
            }
        }

        Ok(health_info)
    }

    /// Count streams in a group
    async fn count_streams_in_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<usize> {
        let streams = self.global_state.streams.read().await;
        let stream_configs = self.global_state.stream_configs.read().await;
        let count = streams
            .keys()
            .filter(|name| {
                stream_configs
                    .get(*name)
                    .and_then(|config| config.consensus_group)
                    .map(|id| id == group_id)
                    .unwrap_or(false)
            })
            .count();
        Ok(count)
    }

    /// Get groups by health status
    pub async fn get_groups_by_status(
        &self,
        status: HealthStatus,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let all_health = self.get_all_group_health().await?;
        Ok(all_health
            .into_iter()
            .filter(|h| h.status == status)
            .map(|h| h.group_id)
            .collect())
    }
}

/// Read-only view of node state
#[derive(Clone)]
pub struct NodeView<G>
where
    G: Governance,
{
    topology_manager: Arc<TopologyManager<G>>,
    global_state: Arc<GlobalState>,
}

impl<G> NodeView<G>
where
    G: Governance,
{
    /// Create a new node view
    pub fn new(topology_manager: Arc<TopologyManager<G>>, global_state: Arc<GlobalState>) -> Self {
        Self {
            topology_manager,
            global_state,
        }
    }

    /// Get health information for a specific node
    pub async fn get_node_health(&self, node_id: &NodeId) -> ConsensusResult<Option<NodeHealth>> {
        // Get node from topology
        // Check if node exists in topology
        if self.topology_manager.get_node(node_id).await.is_none() {
            return Ok(None);
        }

        // Count groups this node is in
        let group_count = self.count_groups_for_node(node_id).await?;

        // Simple health determination
        let status = if group_count == 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Ok(Some(NodeHealth {
            node_id: node_id.clone(),
            status,
            group_count,
            cpu_usage: 0.0,     // Would come from metrics
            memory_usage: 0.0,  // Would come from metrics
            storage_usage: 0.0, // Would come from metrics
            network_latency: HashMap::new(),
            is_reachable: true,
            last_seen: SystemTime::now(),
        }))
    }

    /// Get health information for all nodes
    pub async fn get_all_node_health(&self) -> ConsensusResult<Vec<NodeHealth>> {
        let nodes = self.topology_manager.get_all_peers().await;
        let mut health_info = Vec::new();

        for node in nodes {
            let node_id = NodeId::new(node.public_key());
            if let Some(health) = self.get_node_health(&node_id).await? {
                health_info.push(health);
            }
        }

        Ok(health_info)
    }

    /// Count groups that a node participates in
    async fn count_groups_for_node(&self, node_id: &NodeId) -> ConsensusResult<usize> {
        let groups = self.global_state.consensus_groups.read().await;
        let count = groups
            .values()
            .filter(|g| g.members.contains(node_id))
            .count();
        Ok(count)
    }

    /// Get nodes by health status
    pub async fn get_nodes_by_status(&self, status: HealthStatus) -> ConsensusResult<Vec<NodeId>> {
        let all_health = self.get_all_node_health().await?;
        Ok(all_health
            .into_iter()
            .filter(|h| h.status == status)
            .map(|h| h.node_id)
            .collect())
    }
}

/// System-wide view combining all component views
#[derive(Clone)]
pub struct SystemView<T, G>
where
    T: Transport,
    G: Governance,
{
    pub stream_view: StreamView<T, G>,
    pub group_view: GroupView<T, G>,
    pub node_view: NodeView<G>,
}

impl<T, G> SystemView<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new system view
    pub fn new(
        global_state: Arc<GlobalState>,
        groups_manager: Arc<RwLock<GroupsManager<T, G>>>,
        topology_manager: Arc<TopologyManager<G>>,
    ) -> Self {
        Self {
            stream_view: StreamView::new(global_state.clone(), groups_manager.clone()),
            group_view: GroupView::new(global_state.clone(), groups_manager),
            node_view: NodeView::new(topology_manager, global_state),
        }
    }

    /// Get overall system health
    pub async fn get_system_health(&self) -> ConsensusResult<HealthStatus> {
        // Check streams
        let stream_health = self.stream_view.get_all_stream_health().await?;
        let unhealthy_streams = stream_health
            .iter()
            .filter(|h| h.status == HealthStatus::Unhealthy)
            .count();

        // Check groups
        let group_health = self.group_view.get_all_group_health().await?;
        let unhealthy_groups = group_health
            .iter()
            .filter(|h| h.status == HealthStatus::Unhealthy)
            .count();

        // Check nodes
        let node_health = self.node_view.get_all_node_health().await?;
        let unhealthy_nodes = node_health
            .iter()
            .filter(|h| h.status == HealthStatus::Unhealthy)
            .count();

        // Determine overall health
        if unhealthy_groups > 0 || unhealthy_nodes > 0 {
            Ok(HealthStatus::Unhealthy)
        } else if unhealthy_streams > 0 {
            Ok(HealthStatus::Degraded)
        } else {
            Ok(HealthStatus::Healthy)
        }
    }

    /// Get system summary
    pub async fn get_system_summary(&self) -> ConsensusResult<SystemSummary> {
        let stream_count = self.stream_view.get_all_stream_health().await?.len();
        let group_count = self.group_view.get_all_group_health().await?.len();
        let node_count = self.node_view.get_all_node_health().await?.len();
        let system_health = self.get_system_health().await?;

        Ok(SystemSummary {
            stream_count,
            group_count,
            node_count,
            system_health,
            timestamp: SystemTime::now(),
        })
    }
}

/// System summary information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSummary {
    /// Total number of streams
    pub stream_count: usize,
    /// Total number of groups
    pub group_count: usize,
    /// Total number of nodes
    pub node_count: usize,
    /// Overall system health
    pub system_health: HealthStatus,
    /// Summary generation timestamp
    pub timestamp: SystemTime,
}
