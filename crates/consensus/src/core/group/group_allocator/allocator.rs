//! Group allocation algorithm for managing consensus groups
//!
//! This module implements an intelligent allocator that:
//! - Creates region-specific consensus groups
//! - Maximizes availability zone distribution within groups
//! - Automatically scales groups up and down based on node availability
//! - Handles node rebalancing with dual-membership support

use crate::error::{ConsensusResult, Error};
use crate::{ConsensusGroupId, Node};
use proven_governance::Governance;
use proven_topology::NodeId;
use proven_topology::TopologyManager;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::rebalancer::{GroupRebalancer, RebalancingPlan};
use super::scoring::AllocationScorer;
use super::topology::TopologyAnalyzer;

/// Configuration for the group allocator
#[derive(Debug, Clone)]
pub struct GroupAllocatorConfig {
    /// Minimum number of nodes per group
    pub min_group_size: usize,
    /// Maximum number of nodes per group
    pub max_group_size: usize,
    /// Target size for optimal performance
    pub target_group_size: usize,
    /// Minimum number of groups per region
    pub min_groups_per_region: u32,
    /// Imbalance threshold to trigger rebalancing (0.0 to 1.0)
    pub rebalance_threshold: f64,
    /// Maximum number of groups a node can be in during migration
    pub max_concurrent_groups: usize,
}

impl Default for GroupAllocatorConfig {
    fn default() -> Self {
        Self {
            min_group_size: 3,
            max_group_size: 7,
            target_group_size: 5,
            min_groups_per_region: 1,
            rebalance_threshold: 0.3,
            max_concurrent_groups: 2,
        }
    }
}

/// Tracks current group allocations
#[derive(Debug, Clone, Default)]
pub struct GroupAllocations {
    /// Groups indexed by ID
    pub groups: HashMap<ConsensusGroupId, ConsensusGroup>,
    /// Groups indexed by region
    pub groups_by_region: HashMap<String, HashSet<ConsensusGroupId>>,
    /// Node to groups mapping (supports multiple groups during migration)
    pub node_groups: HashMap<NodeId, HashSet<ConsensusGroupId>>,
    /// Next group ID to assign
    pub next_group_id: u64,
}

impl GroupAllocations {
    /// Create new empty allocations
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            groups_by_region: HashMap::new(),
            node_groups: HashMap::new(),
            next_group_id: 1,
        }
    }

    /// Create a new group and return its ID
    pub fn create_group(&mut self, region: String, members: HashSet<NodeId>) -> ConsensusGroupId {
        let group_id = ConsensusGroupId::new(self.next_group_id as u32);
        self.next_group_id += 1;

        let group = ConsensusGroup {
            id: group_id,
            region: region.clone(),
            members: members.clone(),
            state: GroupState::Active,
        };

        // Update indices
        self.groups.insert(group_id, group);
        self.groups_by_region
            .entry(region)
            .or_default()
            .insert(group_id);

        for node_id in members {
            self.node_groups
                .entry(node_id)
                .or_default()
                .insert(group_id);
        }

        group_id
    }

    /// Remove a group
    pub fn remove_group(&mut self, group_id: ConsensusGroupId) -> Option<ConsensusGroup> {
        if let Some(group) = self.groups.remove(&group_id) {
            // Remove from region index
            if let Some(region_groups) = self.groups_by_region.get_mut(&group.region) {
                region_groups.remove(&group_id);
                if region_groups.is_empty() {
                    self.groups_by_region.remove(&group.region);
                }
            }

            // Remove from node mappings
            for node_id in &group.members {
                if let Some(node_groups) = self.node_groups.get_mut(node_id) {
                    node_groups.remove(&group_id);
                    if node_groups.is_empty() {
                        self.node_groups.remove(node_id);
                    }
                }
            }

            Some(group)
        } else {
            None
        }
    }

    /// Add a node to a group
    pub fn add_node_to_group(
        &mut self,
        node_id: NodeId,
        group_id: ConsensusGroupId,
    ) -> Result<(), String> {
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group {group_id:?} not found"))?;

        group.members.insert(node_id.clone());
        self.node_groups
            .entry(node_id)
            .or_default()
            .insert(group_id);

        Ok(())
    }

    /// Remove a node from a group
    pub fn remove_node_from_group(
        &mut self,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
    ) -> Result<(), String> {
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| format!("Group {group_id:?} not found"))?;

        if !group.members.remove(node_id) {
            return Err(format!("Node {node_id:?} not in group {group_id:?}"));
        }

        if let Some(node_groups) = self.node_groups.get_mut(node_id) {
            node_groups.remove(&group_id);
            if node_groups.is_empty() {
                self.node_groups.remove(node_id);
            }
        }

        Ok(())
    }

    /// Get all groups for a node
    pub fn get_node_groups(&self, node_id: &NodeId) -> Vec<ConsensusGroupId> {
        self.node_groups
            .get(node_id)
            .map(|groups| groups.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get all groups in a region
    pub fn get_region_groups(&self, region: &str) -> Vec<ConsensusGroupId> {
        self.groups_by_region
            .get(region)
            .map(|groups| groups.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// Represents a consensus group
#[derive(Debug, Clone)]
pub struct ConsensusGroup {
    /// Group ID
    pub id: ConsensusGroupId,
    /// Region this group belongs to
    pub region: String,
    /// Current members
    pub members: HashSet<NodeId>,
    /// Current state
    pub state: GroupState,
}

/// State of a consensus group
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// Group is active and healthy
    Active,
    /// Group is being created
    Creating,
    /// Group is being removed
    Removing,
    /// Group is undergoing rebalancing
    Rebalancing,
}

/// Main group allocator
pub struct GroupAllocator<G>
where
    G: Governance,
{
    /// Configuration
    config: GroupAllocatorConfig,
    /// Topology manager for cluster topology
    topology_manager: Arc<TopologyManager<G>>,
    /// Current allocations
    allocations: Arc<RwLock<GroupAllocations>>,
    /// Rebalancer for managing migrations
    rebalancer: Arc<RwLock<GroupRebalancer>>,
}

impl<G> GroupAllocator<G>
where
    G: Governance,
{
    /// Create a new group allocator with a topology manager
    pub fn new(config: GroupAllocatorConfig, topology_manager: Arc<TopologyManager<G>>) -> Self {
        let rebalancer = GroupRebalancer::new(config.clone());
        Self {
            config,
            topology_manager,
            allocations: Arc::new(RwLock::new(GroupAllocations::new())),
            rebalancer: Arc::new(RwLock::new(rebalancer)),
        }
    }

    /// Allocate groups for a new node
    pub async fn allocate_groups_for_node(
        &self,
        node_id: &NodeId,
        node: &Node,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let mut allocations = self.allocations.write().await;

        // Check if node already has groups
        let existing_groups = allocations.get_node_groups(node_id);
        if !existing_groups.is_empty() {
            warn!(
                "Node {:?} already has {} groups assigned",
                node_id,
                existing_groups.len()
            );
            return Ok(existing_groups);
        }

        // Find groups in the node's region
        let region_groups = allocations.get_region_groups(node.region());

        info!(
            "Allocating groups for node {:?} in region {} (found {} existing groups)",
            node_id,
            node.region(),
            region_groups.len()
        );

        // If no groups exist in the region, create one
        if region_groups.is_empty() {
            let group_id = self
                .create_initial_group_for_region(node.region(), node_id.clone(), &mut allocations)
                .await?;
            return Ok(vec![group_id]);
        }

        // Score all existing groups
        let scorer = AllocationScorer::new();
        let scores = scorer
            .score_all_groups_for_node(
                node_id,
                &self.topology_manager,
                &allocations,
                self.config.target_group_size,
            )
            .await?;

        // Check if we should create a new group instead
        let should_create_new = self
            .should_create_new_group(node.region(), &allocations)
            .await?;

        if should_create_new {
            info!(
                "Creating new group for node {:?} in region {}",
                node_id,
                node.region()
            );
            let available_nodes = self
                .get_available_nodes_for_new_group(node.region(), &allocations)
                .await?;

            if available_nodes.len() >= self.config.min_group_size {
                let selected_nodes = scorer
                    .select_nodes_for_new_group(
                        node.region(),
                        &available_nodes,
                        &self.topology_manager,
                        &allocations,
                        self.config.target_group_size,
                        self.config.min_group_size,
                    )
                    .await?;

                if selected_nodes.len() >= self.config.min_group_size {
                    let group_id = allocations.create_group(
                        node.region().to_string(),
                        selected_nodes.into_iter().collect(),
                    );
                    return Ok(vec![group_id]);
                }
            }
        }

        // Otherwise, find the best existing group
        if let Some((best_group_id, best_score)) = scores.first()
            && best_score.total_score > 0.3
        {
            // Minimum acceptable score
            allocations
                .add_node_to_group(node_id.clone(), *best_group_id)
                .map_err(Error::InvalidOperation)?;
            info!(
                "Assigned node {:?} to group {:?} with score {:.2}",
                node_id, best_group_id, best_score.total_score
            );
            return Ok(vec![*best_group_id]);
        }

        // If no suitable group found, create a new one
        warn!(
            "No suitable group found for node {:?}, creating new group",
            node_id
        );
        let group_id = allocations.create_group(
            node.region().to_string(),
            vec![node_id.clone()].into_iter().collect(),
        );
        Ok(vec![group_id])
    }

    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> ConsensusResult<bool> {
        let allocations = self.allocations.read().await;

        // Check each region for imbalance
        for (region, group_ids) in &allocations.groups_by_region {
            let analysis = TopologyAnalyzer::analyze_region(
                &self.topology_manager,
                &allocations,
                region,
                self.config.target_group_size,
            )
            .await?;

            // Check if AZ balance is poor
            if analysis.az_balance_score > self.config.rebalance_threshold {
                return Ok(true);
            }

            // Check group size variance
            let sizes: Vec<usize> = group_ids
                .iter()
                .filter_map(|gid| allocations.groups.get(gid))
                .map(|g| g.members.len())
                .collect();

            if !sizes.is_empty() {
                let mean = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
                let variance = sizes
                    .iter()
                    .map(|&size| (size as f64 - mean).powi(2))
                    .sum::<f64>()
                    / sizes.len() as f64;
                let cv = (variance.sqrt() / mean).min(1.0);

                if cv > self.config.rebalance_threshold {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Get current allocations
    pub async fn get_allocations(&self) -> GroupAllocations {
        self.allocations.read().await.clone()
    }

    /// Get topology manager reference
    pub fn get_topology_manager(&self) -> &Arc<TopologyManager<G>> {
        &self.topology_manager
    }

    /// Create the first group in a region
    async fn create_initial_group_for_region(
        &self,
        region: &str,
        initial_node: NodeId,
        allocations: &mut GroupAllocations,
    ) -> ConsensusResult<ConsensusGroupId> {
        info!(
            "Creating initial group for region {} with node {:?}",
            region, initial_node
        );

        let group_id =
            allocations.create_group(region.to_string(), vec![initial_node].into_iter().collect());

        Ok(group_id)
    }

    /// Check if we should create a new group in a region
    async fn should_create_new_group(
        &self,
        region: &str,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<bool> {
        let current_groups = allocations.get_region_groups(region).len();
        let total_nodes = self
            .topology_manager
            .get_node_count_by_region(region)
            .await?;

        // Need more groups if we have many nodes per group
        let avg_nodes_per_group = if current_groups > 0 {
            total_nodes as f64 / current_groups as f64
        } else {
            f64::INFINITY
        };

        // Create new group if:
        // 1. Average group size exceeds target
        // 2. We have fewer than minimum groups for the region
        Ok(avg_nodes_per_group > self.config.target_group_size as f64
            || (current_groups as u32) < self.config.min_groups_per_region)
    }

    /// Get nodes available for creating a new group
    async fn get_available_nodes_for_new_group(
        &self,
        region: &str,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Vec<NodeId>> {
        let mut available = Vec::new();

        let region_nodes = self.topology_manager.get_nodes_by_region(region).await?;

        for node in region_nodes {
            let node_id = NodeId::from(node.public_key());
            let current_memberships = allocations.get_node_groups(&node_id).len();
            // Only include nodes that aren't at max concurrent groups
            if current_memberships < self.config.max_concurrent_groups {
                available.push(node_id);
            }
        }

        Ok(available)
    }

    /// Remove a node from all its groups
    pub async fn remove_node(&self, node_id: &NodeId) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let mut allocations = self.allocations.write().await;

        // Get all groups the node belongs to
        let node_groups = allocations.get_node_groups(node_id);

        // Note: TopologyManager handles topology updates through governance
        // We don't need to manually remove from topology here

        // Remove from each group
        for group_id in &node_groups {
            allocations
                .remove_node_from_group(node_id, *group_id)
                .map_err(Error::InvalidOperation)?;

            // Check if group is now too small
            if let Some(group) = allocations.groups.get(group_id)
                && group.members.len() < self.config.min_group_size
            {
                warn!(
                    "Group {:?} now has {} members, below minimum {}",
                    group_id,
                    group.members.len(),
                    self.config.min_group_size
                );
                // TODO: Trigger group merge or rebalancing
            }
        }

        Ok(node_groups)
    }

    /// Get recommendations for group operations
    pub async fn get_recommendations(
        &self,
    ) -> ConsensusResult<Vec<super::topology::GroupRecommendation>> {
        let allocations = self.allocations.read().await;
        let mut recommendations = Vec::new();

        // Check each region
        for (region, group_ids) in &allocations.groups_by_region {
            let analysis = TopologyAnalyzer::analyze_region(
                &self.topology_manager,
                &allocations,
                region,
                self.config.target_group_size,
            )
            .await?;

            // Recommend creating groups if needed
            if analysis.current_groups < analysis.recommended_groups {
                recommendations.push(super::topology::GroupRecommendation {
                    action: super::topology::RecommendedAction::CreateGroup {
                        region: region.clone(),
                    },
                    priority: 0.8,
                    reason: format!(
                        "Region {} has {} groups but needs {} for {} nodes",
                        region,
                        analysis.current_groups,
                        analysis.recommended_groups,
                        analysis.total_nodes
                    ),
                });
            }

            // Check for underutilized groups
            for group_id in group_ids {
                if let Some(group) = allocations.groups.get(group_id)
                    && group.members.len() < self.config.min_group_size
                {
                    recommendations.push(super::topology::GroupRecommendation {
                        action: super::topology::RecommendedAction::RemoveGroup {
                            group_id: *group_id,
                        },
                        priority: 0.9,
                        reason: format!(
                            "Group {:?} has only {} members, below minimum {}",
                            group_id,
                            group.members.len(),
                            self.config.min_group_size
                        ),
                    });
                }
            }
        }

        Ok(recommendations)
    }

    /// Generate a rebalancing plan
    pub async fn generate_rebalancing_plan(&self) -> ConsensusResult<Option<RebalancingPlan>> {
        let allocations = self.allocations.read().await;
        let rebalancer = self.rebalancer.read().await;

        rebalancer
            .generate_plan(&self.topology_manager, &allocations)
            .await
    }

    /// Start a node migration (dual membership)
    pub async fn start_node_migration(
        &self,
        node_id: &NodeId,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let mut allocations = self.allocations.write().await;
        let mut rebalancer = self.rebalancer.write().await;

        // Start tracking the migration
        rebalancer.start_migration(node_id.clone(), from_group, to_group)?;

        // Add node to target group (dual membership)
        allocations
            .add_node_to_group(node_id.clone(), to_group)
            .map_err(Error::InvalidOperation)?;

        info!(
            "Started migration: node {:?} added to target group {:?} (dual membership with {:?})",
            node_id, to_group, from_group
        );

        Ok(())
    }

    /// Complete a node migration (remove from source group)
    pub async fn complete_node_migration(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut allocations = self.allocations.write().await;
        let mut rebalancer = self.rebalancer.write().await;

        // Get migration state
        let migration_state = rebalancer
            .get_migration_state(node_id)
            .ok_or_else(|| Error::InvalidOperation(format!("Node {node_id:?} is not migrating")))?
            .clone();

        // Remove from source group
        allocations
            .remove_node_from_group(node_id, migration_state.from_group)
            .map_err(Error::InvalidOperation)?;

        // Mark migration as complete
        rebalancer.update_migration_phase(node_id, super::rebalancer::MigrationPhase::Complete)?;

        info!(
            "Completed migration: node {:?} removed from source group {:?}",
            node_id, migration_state.from_group
        );

        Ok(())
    }

    /// Check if a node is currently migrating
    pub async fn is_node_migrating(&self, node_id: &NodeId) -> bool {
        let rebalancer = self.rebalancer.read().await;
        rebalancer.is_migrating(node_id)
    }

    /// Get all nodes currently in migration
    pub async fn get_migrating_nodes(&self) -> Vec<NodeId> {
        let rebalancer = self.rebalancer.read().await;
        rebalancer.get_migrating_nodes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ClusterTopology tests removed - now using TopologyManager

    #[test]
    fn test_group_allocations() {
        let mut allocations = GroupAllocations::new();

        // Create a group
        // Create test NodeIds from hex strings
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);
        let members = vec![node1.clone(), node2.clone()].into_iter().collect();
        let group_id = allocations.create_group("us-east-1".to_string(), members);

        assert_eq!(allocations.get_node_groups(&node1).len(), 1);
        assert_eq!(allocations.get_region_groups("us-east-1").len(), 1);

        // Remove the group
        allocations.remove_group(group_id);
        assert_eq!(allocations.get_node_groups(&node1).len(), 0);
        assert_eq!(allocations.get_region_groups("us-east-1").len(), 0);
    }
}
