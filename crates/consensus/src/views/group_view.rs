//! Read-only view of consensus group state and health
//!
//! The GroupView provides read-only access to group state for the orchestrator.
//! It tracks:
//! - Group health and load metrics
//! - Group membership
//! - Group configurations
//!
//! All actual modifications must go through consensus operations.

use crate::{
    NodeId,
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error, GroupError},
    global::global_state::{ConsensusGroupInfo, GlobalState},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Group health status
#[derive(Debug, Clone, PartialEq)]
pub enum GroupHealth {
    /// Group has sufficient nodes and is operating normally
    Healthy,
    /// Group is operational but below ideal node count
    UnderReplicated {
        /// The current number of nodes in the group
        current: usize,
        /// The target number of nodes in the group
        target: usize,
    },
    /// Group has no leader
    NoLeader,
    /// Group has insufficient nodes to maintain consensus
    Critical {
        /// The reason the group is critical
        reason: String,
    },
    /// Group has no members
    Empty,
    /// Group health is unknown
    Unknown,
}

/// Group load metrics
#[derive(Debug, Clone, Default)]
pub struct GroupLoad {
    /// Number of streams in the group
    pub stream_count: usize,
    /// Total messages across all streams
    pub total_messages: u64,
    /// Approximate storage size in bytes
    pub storage_bytes: u64,
    /// Number of active operations
    pub active_operations: usize,
    /// Load score (0.0 = no load, 1.0 = fully loaded)
    pub load_score: f64,
}

/// Group configuration
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Minimum number of nodes for the group
    pub min_nodes: usize,
    /// Target number of nodes for the group
    pub target_nodes: usize,
    /// Maximum number of nodes for the group
    pub max_nodes: usize,
    /// Maximum number of streams per group
    pub max_streams: usize,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            min_nodes: 1,
            target_nodes: 3,
            max_nodes: 5,
            max_streams: 1000,
        }
    }
}

/// Group health information
#[derive(Debug, Clone)]
pub struct GroupHealthInfo {
    /// Whether the group is healthy
    pub is_healthy: bool,
    /// Health status
    pub health: GroupHealth,
    /// Number of members
    pub member_count: usize,
    /// Number of streams
    pub stream_count: u32,
}

/// Read-only view of consensus group state for orchestrator decision-making
pub struct GroupView {
    /// Reference to global state
    global_state: Arc<GlobalState>,
    /// Group configurations
    group_configs: Arc<RwLock<HashMap<ConsensusGroupId, GroupConfig>>>,
    /// Group health tracking
    group_health: Arc<RwLock<HashMap<ConsensusGroupId, GroupHealth>>>,
    /// Group load metrics
    group_loads: Arc<RwLock<HashMap<ConsensusGroupId, GroupLoad>>>,
    /// Default group configuration
    default_config: GroupConfig,
}

impl GroupView {
    /// Create a new GroupView
    pub fn new(global_state: Arc<GlobalState>, min_nodes_for_health: usize) -> Self {
        let default_config = GroupConfig {
            min_nodes: min_nodes_for_health,
            ..Default::default()
        };

        Self {
            global_state,
            group_configs: Arc::new(RwLock::new(HashMap::new())),
            group_health: Arc::new(RwLock::new(HashMap::new())),
            group_loads: Arc::new(RwLock::new(HashMap::new())),
            default_config,
        }
    }

    /// Validate group creation parameters
    pub async fn validate_group_creation(
        &self,
        group_id: ConsensusGroupId,
        members: &[NodeId],
    ) -> ConsensusResult<()> {
        // Check if group already exists
        if self.global_state.get_group(group_id).await.is_some() {
            return Err(Error::Group(GroupError::AlreadyExists { id: group_id }));
        }

        // Validate member count
        let config = self.default_config.clone();
        if members.len() < config.min_nodes {
            return Err(Error::Group(GroupError::InsufficientMembers {
                required: config.min_nodes,
                actual: members.len(),
            }));
        }

        if members.len() > config.max_nodes {
            return Err(Error::Group(GroupError::TooManyMembers {
                max: config.max_nodes,
                actual: members.len(),
            }));
        }

        Ok(())
    }

    /// Create a new consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        initial_members: Vec<NodeId>,
        config: Option<GroupConfig>,
    ) -> ConsensusResult<()> {
        // Check if group already exists
        if self.global_state.get_group(group_id).await.is_some() {
            return Err(Error::Group(GroupError::AlreadyExists { id: group_id }));
        }

        // Validate initial members
        if initial_members.is_empty() {
            return Err(Error::Group(GroupError::NoMembers { id: group_id }));
        }

        let config = config.unwrap_or_else(|| self.default_config.clone());

        // Validate member count against config
        if initial_members.len() > config.max_nodes {
            return Err(Error::Group(GroupError::AtCapacity {
                id: group_id,
                max: config.max_nodes,
            }));
        }

        // Create consensus group info
        let _group = ConsensusGroupInfo {
            id: group_id,
            members: initial_members.clone(),
            created_at: chrono::Utc::now().timestamp() as u64,
            stream_count: 0,
        };

        // We can't directly add groups - this needs to be done through consensus
        // For now, just track in our local state
        info!(
            "Group creation for {:?} must be done through consensus operations",
            group_id
        );

        // Store configuration
        self.group_configs.write().await.insert(group_id, config);

        // Initialize health
        self.update_health(group_id).await?;

        // Initialize load metrics
        self.group_loads
            .write()
            .await
            .insert(group_id, GroupLoad::default());

        info!("Created consensus group {:?}", group_id);
        Ok(())
    }

    /// Add a node to a group
    pub async fn add_node_to_group(
        &self,
        group_id: ConsensusGroupId,
        node_id: NodeId,
    ) -> ConsensusResult<()> {
        let group = self
            .global_state
            .get_group(group_id)
            .await
            .ok_or(Error::Group(GroupError::NotFound { id: group_id }))?;

        // Check if node is already in group
        if group.members.iter().any(|m| m == &node_id) {
            return Err(Error::Node(crate::error::NodeError::AlreadyInGroup {
                node_id,
                group_id,
            }));
        }

        // Check capacity
        let config = self.get_group_config(group_id).await;
        if group.members.len() >= config.max_nodes {
            return Err(Error::Group(GroupError::AtCapacity {
                id: group_id,
                max: config.max_nodes,
            }));
        }

        // Note: Adding nodes to groups must be done through consensus operations
        // The managers are read-only views - actual modifications go through orchestrator
        info!(
            "Node addition to group {:?} must be done through consensus operations",
            group_id
        );

        // Update health
        self.update_health(group_id).await?;

        info!("Added node {} to group {:?}", node_id, group_id);
        Ok(())
    }

    /// Remove a node from a group
    pub async fn remove_node_from_group(
        &self,
        group_id: ConsensusGroupId,
        node_id: &NodeId,
    ) -> ConsensusResult<()> {
        let group = self
            .global_state
            .get_group(group_id)
            .await
            .ok_or(Error::Group(GroupError::NotFound { id: group_id }))?;

        // Check if node is in group
        if !group.members.iter().any(|m| m == node_id) {
            return Err(Error::Node(crate::error::NodeError::NotInGroup {
                node_id: node_id.clone(),
                group_id,
            }));
        }

        // Note: Removing nodes from groups must be done through consensus operations
        // The managers are read-only views - actual modifications go through orchestrator
        info!(
            "Node removal from group {:?} must be done through consensus operations",
            group_id
        );

        // Check if group is now empty
        if let Some(updated_group) = self.global_state.get_group(group_id).await {
            if updated_group.members.is_empty() {
                // Mark as empty but don't delete yet - let orchestrator decide
                self.group_health
                    .write()
                    .await
                    .insert(group_id, GroupHealth::Empty);
                warn!("Group {:?} is now empty", group_id);
            }
        }

        // Update health
        self.update_health(group_id).await?;

        info!("Removed node {} from group {:?}", node_id, group_id);
        Ok(())
    }

    /// Delete a consensus group
    pub async fn delete_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        let _group = self
            .global_state
            .get_group(group_id)
            .await
            .ok_or(Error::Group(GroupError::NotFound { id: group_id }))?;

        // Check if group has active streams
        // Check if group has active streams
        // In real implementation, this would integrate with StreamManager
        let stream_count = 0; // Placeholder
        if stream_count > 0 {
            return Err(Error::Group(GroupError::HasActiveStreams {
                id: group_id,
                count: stream_count,
            }));
        }

        // Note: We can't directly remove consensus groups from GlobalState
        // as the method is private. This would need to be done through
        // a proper consensus operation in the orchestrator.
        warn!(
            "Group deletion requested for {:?}, but must be done through consensus",
            group_id
        );

        // Remove tracking
        self.group_configs.write().await.remove(&group_id);
        self.group_health.write().await.remove(&group_id);
        self.group_loads.write().await.remove(&group_id);

        info!("Deleted consensus group {:?}", group_id);
        Ok(())
    }

    /// Get group configuration
    async fn get_group_config(&self, group_id: ConsensusGroupId) -> GroupConfig {
        self.group_configs
            .read()
            .await
            .get(&group_id)
            .cloned()
            .unwrap_or_else(|| self.default_config.clone())
    }

    /// Update group health status
    pub async fn update_health(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        let group = self
            .global_state
            .get_group(group_id)
            .await
            .ok_or(Error::Group(GroupError::NotFound { id: group_id }))?;

        let config = self.get_group_config(group_id).await;
        let member_count = group.members.len();

        let health = if member_count == 0 {
            GroupHealth::Empty
        } else if member_count < config.min_nodes {
            GroupHealth::Critical {
                reason: format!(
                    "Only {} nodes, need at least {}",
                    member_count, config.min_nodes
                ),
            }
        } else if member_count < config.target_nodes {
            GroupHealth::UnderReplicated {
                current: member_count,
                target: config.target_nodes,
            }
        } else {
            GroupHealth::Healthy
        };

        self.group_health
            .write()
            .await
            .insert(group_id, health.clone());

        match &health {
            GroupHealth::Critical { reason } => {
                warn!("Group {:?} is critical: {}", group_id, reason);
            }
            GroupHealth::Empty => {
                warn!("Group {:?} is empty", group_id);
            }
            _ => {}
        }

        Ok(())
    }

    /// Get group health
    pub async fn get_health(&self, group_id: ConsensusGroupId) -> Option<GroupHealth> {
        self.group_health.read().await.get(&group_id).cloned()
    }

    /// Update group load metrics
    pub async fn update_load(&self, group_id: ConsensusGroupId, load: GroupLoad) {
        self.group_loads.write().await.insert(group_id, load);
    }

    /// Get group load
    pub async fn get_load(&self, group_id: ConsensusGroupId) -> Option<GroupLoad> {
        self.group_loads.read().await.get(&group_id).cloned()
    }

    /// Get the least loaded group
    pub async fn get_least_loaded_group(&self) -> Option<ConsensusGroupId> {
        let loads = self.group_loads.read().await;
        let health = self.group_health.read().await;

        loads
            .iter()
            .filter(|(group_id, _)| {
                // Only consider healthy groups
                matches!(
                    health.get(group_id),
                    Some(GroupHealth::Healthy) | Some(GroupHealth::UnderReplicated { .. })
                )
            })
            .min_by(|(_, a), (_, b)| {
                a.load_score
                    .partial_cmp(&b.load_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(id, _)| *id)
    }

    /// List all groups
    pub async fn list_groups(&self) -> Vec<ConsensusGroupId> {
        self.global_state
            .get_all_groups()
            .await
            .into_iter()
            .map(|g| g.id)
            .collect()
    }

    /// List healthy groups
    pub async fn list_healthy_groups(&self) -> Vec<ConsensusGroupId> {
        let health_map = self.group_health.read().await;
        health_map
            .iter()
            .filter(|(_, health)| matches!(health, GroupHealth::Healthy))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get groups that need more nodes
    pub async fn get_under_replicated_groups(&self) -> Vec<(ConsensusGroupId, usize, usize)> {
        let health_map = self.group_health.read().await;
        health_map
            .iter()
            .filter_map(|(id, health)| match health {
                GroupHealth::UnderReplicated { current, target } => Some((*id, *current, *target)),
                _ => None,
            })
            .collect()
    }

    /// Count streams in a group (placeholder - would integrate with StreamManager)
    #[allow(dead_code)]
    async fn count_streams_in_group(&self, _group_id: ConsensusGroupId) -> usize {
        // In real implementation, this would query StreamManager
        // For now, return 0 as placeholder
        0
    }

    /// Calculate load score for a group
    pub async fn calculate_load_score(&self, group_id: ConsensusGroupId) -> f64 {
        let load = self.get_load(group_id).await.unwrap_or_default();
        let config = self.get_group_config(group_id).await;

        // Simple load calculation based on stream count
        let stream_load = (load.stream_count as f64) / (config.max_streams as f64);

        // Could add more factors like message rate, storage, etc.
        stream_load.min(1.0)
    }

    /// Monitor health of all groups
    pub async fn monitor_group_health(&self) {
        let groups = self.list_groups().await;

        for group_id in groups {
            if let Err(e) = self.update_health(group_id).await {
                warn!("Failed to update health for group {:?}: {}", group_id, e);
            }
        }
    }

    /// Get failed groups
    pub async fn get_failed_groups(&self) -> Vec<ConsensusGroupId> {
        let health_map = self.group_health.read().await;
        health_map
            .iter()
            .filter(|(_, health)| {
                matches!(health, GroupHealth::Critical { .. } | GroupHealth::Empty)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Calculate load imbalance ratio across all groups
    pub async fn calculate_load_imbalance(&self) -> f64 {
        let loads = self.group_loads.read().await;

        if loads.is_empty() {
            return 0.0;
        }

        let stream_counts: Vec<usize> = loads.values().map(|load| load.stream_count).collect();

        if stream_counts.is_empty() {
            return 0.0;
        }

        let max = stream_counts.iter().max().copied().unwrap_or(0) as f64;
        let min = stream_counts.iter().min().copied().unwrap_or(0) as f64;
        let avg = stream_counts.iter().sum::<usize>() as f64 / stream_counts.len() as f64;

        if avg > 0.0 { (max - min) / avg } else { 0.0 }
    }

    /// Get group loads
    pub async fn get_group_loads(&self) -> HashMap<ConsensusGroupId, GroupLoad> {
        self.group_loads.read().await.clone()
    }

    /// Get all group health information
    pub async fn get_all_group_health(&self) -> HashMap<ConsensusGroupId, GroupHealthInfo> {
        let health_map = self.group_health.read().await;
        let loads = self.group_loads.read().await;
        let groups = self.global_state.get_all_groups().await;

        groups
            .into_iter()
            .map(|group| {
                let health = health_map
                    .get(&group.id)
                    .cloned()
                    .unwrap_or(GroupHealth::Unknown);
                let load = loads.get(&group.id).cloned();

                (
                    group.id,
                    GroupHealthInfo {
                        is_healthy: matches!(health, GroupHealth::Healthy),
                        health,
                        member_count: group.members.len(),
                        stream_count: load.as_ref().map(|l| l.stream_count as u32).unwrap_or(0),
                    },
                )
            })
            .collect()
    }

    /// Get rebalancing recommendations
    pub async fn get_rebalancing_recommendations(
        &self,
        min_streams_to_migrate: usize,
        max_migrations_per_group: usize,
    ) -> Vec<crate::monitoring::RecommendedMigration> {
        let loads = self.group_loads.read().await;
        let health_map = self.group_health.read().await;

        // Find overloaded and underloaded groups
        let avg_load = if !loads.is_empty() {
            loads.values().map(|l| l.stream_count).sum::<usize>() as f64 / loads.len() as f64
        } else {
            return Vec::new();
        };

        let mut overloaded: Vec<_> = loads
            .iter()
            .filter(|(id, load)| {
                matches!(health_map.get(id), Some(GroupHealth::Healthy))
                    && load.stream_count as f64 > avg_load * 1.5
            })
            .collect();

        let underloaded: Vec<_> = loads
            .iter()
            .filter(|(id, load)| {
                matches!(health_map.get(id), Some(GroupHealth::Healthy))
                    && (load.stream_count as f64) < avg_load * 0.5
            })
            .collect();

        let mut recommendations = Vec::new();

        // Sort overloaded groups by load (highest first)
        overloaded.sort_by(|a, b| b.1.stream_count.cmp(&a.1.stream_count));

        for (from_group, from_load) in overloaded {
            if recommendations.len() >= max_migrations_per_group {
                break;
            }

            // Find best target group
            if let Some((to_group, _)) =
                underloaded.iter().min_by_key(|(_, load)| load.stream_count)
            {
                let streams_to_migrate = std::cmp::min(
                    min_streams_to_migrate,
                    ((from_load.stream_count as f64 - avg_load) / 2.0) as usize,
                );

                // In real implementation, would select specific streams
                // For now, create placeholder recommendations
                for i in 0..streams_to_migrate {
                    recommendations.push(crate::monitoring::RecommendedMigration {
                        stream_name: format!("stream-{}-{}", from_group.0, i),
                        from_group: *from_group,
                        to_group: **to_group,
                        balance_improvement: (from_load.stream_count as f64 - avg_load) / avg_load,
                    });
                }
            }
        }

        recommendations
    }
}
