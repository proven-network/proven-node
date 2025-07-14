//! Group rebalancing with dual membership support
//!
//! This module implements the rebalancing strategy that allows nodes to be
//! in multiple groups during transitions to maintain availability.

use crate::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use proven_topology::NodeId;
use std::collections::{HashMap, HashSet};
use tracing::info;

use super::allocator::{GroupAllocations, GroupAllocatorConfig};
use super::scoring::AllocationScorer;
use super::topology::TopologyAnalyzer;

/// A rebalancing plan that describes node movements
#[derive(Debug, Clone)]
pub struct RebalancingPlan {
    /// Migrations to perform (node -> from group -> to group)
    pub migrations: Vec<NodeMigration>,
    /// Groups to be removed after migrations complete
    pub groups_to_remove: Vec<ConsensusGroupId>,
    /// New groups to create before migrations
    pub groups_to_create: Vec<GroupCreation>,
}

/// A single node migration
#[derive(Debug, Clone)]
pub struct NodeMigration {
    /// Node to migrate
    pub node_id: NodeId,
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
    /// Priority of this migration (higher = more urgent)
    pub priority: f64,
    /// Reason for the migration
    pub reason: String,
}

/// A group creation request
#[derive(Debug, Clone)]
pub struct GroupCreation {
    /// Region for the new group
    pub region: String,
    /// Initial members (if any)
    pub initial_members: Vec<NodeId>,
}

/// Migration state for tracking dual membership
#[derive(Debug, Clone)]
pub struct MigrationState {
    /// Nodes currently in migration (dual membership)
    pub migrating_nodes: HashMap<NodeId, NodeMigrationState>,
    /// Active rebalancing plan
    pub active_plan: Option<RebalancingPlan>,
}

/// State of a single node migration
#[derive(Debug, Clone)]
pub struct NodeMigrationState {
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
    /// When the migration started
    pub started_at: std::time::Instant,
    /// Current phase
    pub phase: MigrationPhase,
}

/// Phases of node migration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Node added to target group (dual membership starts)
    AddedToTarget,
    /// Node syncing with target group
    SyncingWithTarget,
    /// Node ready to leave source group
    ReadyToLeaveSource,
    /// Node removed from source group (migration complete)
    Complete,
}

/// Rebalancer for managing group migrations
pub struct GroupRebalancer {
    config: GroupAllocatorConfig,
    scorer: AllocationScorer,
    migration_state: MigrationState,
}

impl GroupRebalancer {
    /// Create a new rebalancer
    pub fn new(config: GroupAllocatorConfig) -> Self {
        Self {
            config,
            scorer: AllocationScorer::new(),
            migration_state: MigrationState {
                migrating_nodes: HashMap::new(),
                active_plan: None,
            },
        }
    }

    /// Generate a rebalancing plan based on current state
    pub async fn generate_plan<G>(
        &self,
        topology_manager: &proven_topology::TopologyManager<G>,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Option<RebalancingPlan>>
    where
        G: proven_governance::Governance,
    {
        let mut migrations = Vec::new();
        let mut groups_to_remove = Vec::new();
        let groups_to_create = Vec::new();

        // Analyze each region
        for (region, group_ids) in &allocations.groups_by_region {
            let analysis = TopologyAnalyzer::analyze_region(
                topology_manager,
                allocations,
                region,
                self.config.target_group_size,
            )
            .await?;

            // Check for poor AZ balance
            if analysis.az_balance_score > self.config.rebalance_threshold {
                info!(
                    "Region {} has poor AZ balance (score: {:.2}), planning rebalance",
                    region, analysis.az_balance_score
                );

                // Find migrations to improve balance
                let region_migrations = self
                    .plan_region_rebalance(region, group_ids, topology_manager, allocations)
                    .await?;
                migrations.extend(region_migrations);
            }

            // Check for undersize groups
            for group_id in group_ids {
                if let Some(group) = allocations.groups.get(group_id)
                    && group.members.len() < self.config.min_group_size
                {
                    // Plan to merge or remove this group
                    let group_migrations = self
                        .plan_group_removal(*group_id, topology_manager, allocations)
                        .await?;
                    migrations.extend(group_migrations);
                    groups_to_remove.push(*group_id);
                }
            }
        }

        if migrations.is_empty() && groups_to_remove.is_empty() && groups_to_create.is_empty() {
            return Ok(None);
        }

        Ok(Some(RebalancingPlan {
            migrations,
            groups_to_remove,
            groups_to_create,
        }))
    }

    /// Plan rebalancing for a region to improve AZ distribution
    async fn plan_region_rebalance<G>(
        &self,
        region: &str,
        group_ids: &HashSet<ConsensusGroupId>,
        topology_manager: &proven_topology::TopologyManager<G>,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Vec<NodeMigration>>
    where
        G: proven_governance::Governance,
    {
        let mut migrations = Vec::new();

        // Analyze each group's AZ distribution
        let mut group_analyses = Vec::new();
        for group_id in group_ids {
            if let Some(group) = allocations.groups.get(group_id) {
                let analysis = TopologyAnalyzer::analyze_group(
                    topology_manager,
                    group,
                    self.config.target_group_size,
                )
                .await?;
                group_analyses.push((*group_id, analysis));
            }
        }

        // Sort groups by AZ diversity (worst first)
        group_analyses.sort_by(|a, b| {
            a.1.az_diversity_score
                .partial_cmp(&b.1.az_diversity_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Try to move nodes from over-represented AZs to under-represented ones
        for (group_id, analysis) in &group_analyses {
            if analysis.az_diversity_score < 0.5 {
                // Find over-represented AZs in this group
                let avg_per_az = analysis.size as f64 / analysis.az_coverage as f64;

                for (az, &count) in &analysis.az_distribution {
                    if count as f64 > avg_per_az * 1.5 {
                        // This AZ is over-represented, find nodes to move
                        let nodes_to_move = self
                            .find_nodes_to_move(*group_id, az, topology_manager, allocations)
                            .await?;

                        for node_id in nodes_to_move {
                            // Find a better group for this node
                            if let Some(target_group) = self
                                .find_target_group_for_node(
                                    &node_id,
                                    *group_id,
                                    region,
                                    topology_manager,
                                    allocations,
                                )
                                .await?
                            {
                                migrations.push(NodeMigration {
                                    node_id,
                                    from_group: *group_id,
                                    to_group: target_group,
                                    priority: 0.7,
                                    reason: format!(
                                        "Rebalancing AZ {az} distribution in group {group_id:?}"
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(migrations)
    }

    /// Plan migrations for removing an undersize group
    async fn plan_group_removal<G>(
        &self,
        group_id: ConsensusGroupId,
        topology_manager: &proven_topology::TopologyManager<G>,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Vec<NodeMigration>>
    where
        G: proven_governance::Governance,
    {
        let mut migrations = Vec::new();

        let group = allocations
            .groups
            .get(&group_id)
            .ok_or_else(|| Error::InvalidOperation(format!("Group {group_id:?} not found")))?;

        // Find target groups for all members
        for node_id in &group.members {
            if let Some(node) = topology_manager.get_governance_node(node_id).await? {
                // Score all other groups in the region
                let region_groups: Vec<_> = allocations
                    .get_region_groups(&node.region)
                    .into_iter()
                    .filter(|&gid| gid != group_id)
                    .collect();

                if let Some(&best_target) = region_groups.first() {
                    migrations.push(NodeMigration {
                        node_id: node_id.clone(),
                        from_group: group_id,
                        to_group: best_target,
                        priority: 0.9,
                        reason: format!(
                            "Group {:?} is below minimum size ({})",
                            group_id,
                            group.members.len()
                        ),
                    });
                }
            }
        }

        Ok(migrations)
    }

    /// Find nodes that can be moved from an over-represented AZ
    async fn find_nodes_to_move<G>(
        &self,
        group_id: ConsensusGroupId,
        az: &str,
        topology_manager: &proven_topology::TopologyManager<G>,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Vec<NodeId>>
    where
        G: proven_governance::Governance,
    {
        let mut candidates = Vec::new();

        if let Some(group) = allocations.groups.get(&group_id) {
            for node_id in &group.members {
                if let Some(node) = topology_manager.get_governance_node(node_id).await?
                    && node.availability_zone == az
                {
                    // Check if this node can be moved (not already migrating)
                    let current_groups = allocations.get_node_groups(node_id).len();
                    if current_groups < self.config.max_concurrent_groups {
                        candidates.push(node_id.clone());
                    }
                }
            }
        }

        // Return up to 1 node to move at a time (conservative approach)
        Ok(candidates.into_iter().take(1).collect())
    }

    /// Find the best target group for a node migration
    async fn find_target_group_for_node<G>(
        &self,
        node_id: &NodeId,
        exclude_group: ConsensusGroupId,
        region: &str,
        topology_manager: &proven_topology::TopologyManager<G>,
        allocations: &GroupAllocations,
    ) -> ConsensusResult<Option<ConsensusGroupId>>
    where
        G: proven_governance::Governance,
    {
        let _node = topology_manager.get_governance_node(node_id).await?;
        if _node.is_none() {
            return Ok(None);
        }

        // Get all groups in the region except the current one
        let region_groups: Vec<_> = allocations
            .get_region_groups(region)
            .into_iter()
            .filter(|&gid| gid != exclude_group)
            .collect();

        // Score each potential target
        let mut best_group = None;
        let mut best_score = 0.0;

        for &group_id in &region_groups {
            if let Some(group) = allocations.groups.get(&group_id) {
                // Skip if group is full
                if group.members.len() >= self.config.max_group_size {
                    continue;
                }

                let score = self
                    .scorer
                    .score_node_allocation(
                        node_id,
                        group,
                        topology_manager,
                        allocations,
                        self.config.target_group_size,
                    )
                    .await?;

                if score.total_score > best_score {
                    best_score = score.total_score;
                    best_group = Some(group_id);
                }
            }
        }

        Ok(best_group)
    }

    /// Start a node migration (adds node to target group)
    pub fn start_migration(
        &mut self,
        node_id: NodeId,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        if self.migration_state.migrating_nodes.contains_key(&node_id) {
            return Err(Error::InvalidOperation(format!(
                "Node {node_id:?} is already migrating"
            )));
        }

        let state = NodeMigrationState {
            from_group,
            to_group,
            started_at: std::time::Instant::now(),
            phase: MigrationPhase::AddedToTarget,
        };

        self.migration_state
            .migrating_nodes
            .insert(node_id.clone(), state);

        info!(
            "Started migration of node {:?} from group {:?} to {:?}",
            node_id, from_group, to_group
        );

        Ok(())
    }

    /// Update migration phase
    pub fn update_migration_phase(
        &mut self,
        node_id: &NodeId,
        phase: MigrationPhase,
    ) -> ConsensusResult<()> {
        let state = self
            .migration_state
            .migrating_nodes
            .get_mut(node_id)
            .ok_or_else(|| Error::InvalidOperation(format!("Node {node_id:?} is not migrating")))?;

        state.phase = phase;

        info!("Node {:?} migration phase updated to {:?}", node_id, phase);

        if phase == MigrationPhase::Complete {
            self.migration_state.migrating_nodes.remove(node_id);
            info!("Node {:?} migration complete", node_id);
        }

        Ok(())
    }

    /// Get current migration state for a node
    pub fn get_migration_state(&self, node_id: &NodeId) -> Option<&NodeMigrationState> {
        self.migration_state.migrating_nodes.get(node_id)
    }

    /// Check if a node is currently migrating
    pub fn is_migrating(&self, node_id: &NodeId) -> bool {
        self.migration_state.migrating_nodes.contains_key(node_id)
    }

    /// Get all nodes currently in migration
    pub fn get_migrating_nodes(&self) -> Vec<NodeId> {
        self.migration_state
            .migrating_nodes
            .keys()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_state_tracking() {
        let config = GroupAllocatorConfig::default();
        let mut rebalancer = GroupRebalancer::new(config);

        let node_id = NodeId::from_seed(1);
        let from_group = ConsensusGroupId::new(1);
        let to_group = ConsensusGroupId::new(2);

        // Start migration
        rebalancer
            .start_migration(node_id.clone(), from_group, to_group)
            .unwrap();
        assert!(rebalancer.is_migrating(&node_id));

        // Update phase
        rebalancer
            .update_migration_phase(&node_id, MigrationPhase::SyncingWithTarget)
            .unwrap();
        let state = rebalancer.get_migration_state(&node_id).unwrap();
        assert_eq!(state.phase, MigrationPhase::SyncingWithTarget);

        // Complete migration
        rebalancer
            .update_migration_phase(&node_id, MigrationPhase::Complete)
            .unwrap();
        assert!(!rebalancer.is_migrating(&node_id));
    }
}
