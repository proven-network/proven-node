//! Scoring algorithms for group allocation decisions
//!
//! This module provides scoring functions to evaluate allocation decisions
//! based on multiple factors like AZ diversity, group size, and load balance.

use super::allocator::{ConsensusGroup, GroupAllocations};
use crate::allocation::ConsensusGroupId;
use crate::error::ConsensusResult;
use crate::node_id::NodeId;
use crate::topology::TopologyManager;
use proven_governance::Governance;
use std::collections::HashMap;

/// Configuration for scoring weights
#[derive(Debug, Clone)]
pub struct ScoringConfig {
    /// Weight for AZ diversity (0.0 - 1.0)
    pub az_diversity_weight: f64,
    /// Weight for group size balance (0.0 - 1.0)
    pub size_balance_weight: f64,
    /// Weight for node load balance (0.0 - 1.0)
    pub load_balance_weight: f64,
    /// Weight for minimizing concurrent group membership (0.0 - 1.0)
    pub membership_weight: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            az_diversity_weight: 0.4,
            size_balance_weight: 0.3,
            load_balance_weight: 0.2,
            membership_weight: 0.1,
        }
    }
}

/// Scores for a potential allocation
#[derive(Debug, Clone)]
pub struct AllocationScore {
    /// AZ diversity score (0.0 = poor, 1.0 = excellent)
    pub az_diversity: f64,
    /// Size balance score (0.0 = poor, 1.0 = excellent)
    pub size_balance: f64,
    /// Load balance score (0.0 = poor, 1.0 = excellent)
    pub load_balance: f64,
    /// Membership optimization score (0.0 = poor, 1.0 = excellent)
    pub membership_score: f64,
    /// Combined weighted score (0.0 = poor, 1.0 = excellent)
    pub total_score: f64,
}

/// Allocation scorer
#[derive(Debug, Default)]
pub struct AllocationScorer {
    config: ScoringConfig,
}

impl AllocationScorer {
    /// Create a new allocation scorer with default config
    pub fn new() -> Self {
        Self {
            config: ScoringConfig::default(),
        }
    }

    /// Create a new allocation scorer with custom config
    pub fn with_config(config: ScoringConfig) -> Self {
        Self { config }
    }

    /// Score a potential node allocation to a group
    pub async fn score_node_allocation<G>(
        &self,
        node_id: &NodeId,
        group: &ConsensusGroup,
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        target_group_size: usize,
    ) -> ConsensusResult<AllocationScore>
    where
        G: Governance + Send + Sync + 'static,
    {
        // Get node info
        let node = match topology_manager.get_governance_node(node_id).await? {
            Some(n) => n,
            None => {
                return Ok(AllocationScore {
                    az_diversity: 0.0,
                    size_balance: 0.0,
                    load_balance: 0.0,
                    membership_score: 0.0,
                    total_score: 0.0,
                });
            }
        };

        // Calculate individual scores
        let az_diversity = self
            .calculate_az_diversity_score(group, &node.availability_zone, topology_manager)
            .await?;
        let size_balance =
            self.calculate_size_balance_score(group.members.len() + 1, target_group_size);
        let load_balance = self.calculate_load_balance_score(allocations, &group.region);
        let membership_score = self.calculate_membership_score(node_id, allocations);

        // Calculate weighted total
        let total_score = (az_diversity * self.config.az_diversity_weight)
            + (size_balance * self.config.size_balance_weight)
            + (load_balance * self.config.load_balance_weight)
            + (membership_score * self.config.membership_weight);

        Ok(AllocationScore {
            az_diversity,
            size_balance,
            load_balance,
            membership_score,
            total_score,
        })
    }

    /// Score all possible allocations for a node
    pub async fn score_all_groups_for_node<G>(
        &self,
        node_id: &NodeId,
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        target_group_size: usize,
    ) -> ConsensusResult<Vec<(ConsensusGroupId, AllocationScore)>>
    where
        G: Governance + Send + Sync + 'static,
    {
        let node = match topology_manager.get_governance_node(node_id).await? {
            Some(n) => n,
            None => return Ok(vec![]),
        };

        // Get groups in the node's region
        let region_groups = allocations.get_region_groups(&node.region);

        let mut scores = Vec::new();
        for group_id in region_groups {
            if let Some(group) = allocations.groups.get(&group_id) {
                // Skip if node is already in this group
                if group.members.contains(node_id) {
                    continue;
                }

                let score = self
                    .score_node_allocation(
                        node_id,
                        group,
                        topology_manager,
                        allocations,
                        target_group_size,
                    )
                    .await?;
                scores.push((group_id, score));
            }
        }

        // Sort by total score (descending)
        scores.sort_by(|a, b| {
            b.1.total_score
                .partial_cmp(&a.1.total_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(scores)
    }

    /// Calculate AZ diversity score for adding a node to a group
    async fn calculate_az_diversity_score<G>(
        &self,
        group: &ConsensusGroup,
        new_node_az: &str,
        topology_manager: &TopologyManager<G>,
    ) -> ConsensusResult<f64>
    where
        G: Governance + Send + Sync + 'static,
    {
        // Count current AZ distribution
        let mut az_counts: HashMap<String, usize> = HashMap::new();
        for member_id in &group.members {
            if let Some(member) = topology_manager.get_governance_node(member_id).await? {
                *az_counts
                    .entry(member.availability_zone.clone())
                    .or_insert(0) += 1;
            }
        }

        // Check if adding this node improves diversity
        let current_unique_azs = az_counts.len();
        let new_unique_azs = if az_counts.contains_key(new_node_az) {
            current_unique_azs
        } else {
            current_unique_azs + 1
        };

        // Get total possible AZs in the region
        let total_azs = topology_manager
            .get_azs_in_region(&group.region)
            .await?
            .len();

        if total_azs == 0 {
            return Ok(0.0);
        }

        // Score based on coverage of available AZs
        let coverage_score = new_unique_azs as f64 / total_azs as f64;

        // Bonus for adding a new AZ
        let new_az_bonus = if new_unique_azs > current_unique_azs {
            0.2
        } else {
            0.0
        };

        // Penalty for overloading an AZ
        let current_count = az_counts.get(new_node_az).copied().unwrap_or(0);
        let ideal_per_az = (group.members.len() + 1) as f64 / new_unique_azs as f64;
        let overload_penalty = if current_count as f64 + 1.0 > ideal_per_az * 1.5 {
            0.2
        } else {
            0.0
        };

        Ok((coverage_score + new_az_bonus - overload_penalty).clamp(0.0, 1.0))
    }

    /// Calculate size balance score
    fn calculate_size_balance_score(&self, new_size: usize, target_size: usize) -> f64 {
        if target_size == 0 {
            return 0.0;
        }

        // Perfect score at target size, decreasing as we deviate
        let deviation = (new_size as i32 - target_size as i32).abs() as f64;
        let normalized_deviation = deviation / target_size as f64;

        // Use exponential decay for score
        (-normalized_deviation).exp()
    }

    /// Calculate load balance score across groups in a region
    fn calculate_load_balance_score(&self, allocations: &GroupAllocations, region: &str) -> f64 {
        let region_groups = allocations.get_region_groups(region);
        if region_groups.is_empty() {
            return 1.0; // Perfect score if no groups yet
        }

        // Calculate variance in group sizes
        let sizes: Vec<usize> = region_groups
            .iter()
            .filter_map(|gid| allocations.groups.get(gid))
            .map(|g| g.members.len())
            .collect();

        if sizes.is_empty() {
            return 1.0;
        }

        let mean = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
        let variance = sizes
            .iter()
            .map(|&size| (size as f64 - mean).powi(2))
            .sum::<f64>()
            / sizes.len() as f64;

        // Convert variance to score (lower variance = higher score)
        // Normalize by mean to make it scale-independent
        if mean > 0.0 {
            let cv = (variance.sqrt() / mean).min(1.0); // Coefficient of variation
            1.0 - cv
        } else {
            1.0
        }
    }

    /// Calculate membership score (prefer nodes with fewer group memberships)
    fn calculate_membership_score(&self, node_id: &NodeId, allocations: &GroupAllocations) -> f64 {
        let current_groups = allocations.get_node_groups(node_id).len();

        // Score decreases with more memberships
        match current_groups {
            0 => 1.0,
            1 => 0.8,
            2 => 0.5,
            3 => 0.2,
            _ => 0.0,
        }
    }

    /// Score a group creation decision
    pub async fn score_group_creation<G>(
        &self,
        region: &str,
        proposed_members: &[NodeId],
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        target_size: usize,
    ) -> ConsensusResult<f64>
    where
        G: Governance + Send + Sync + 'static,
    {
        if proposed_members.is_empty() {
            return Ok(0.0);
        }

        // Calculate AZ distribution of proposed members
        let mut az_counts: HashMap<String, usize> = HashMap::new();
        for node_id in proposed_members {
            if let Some(node) = topology_manager.get_governance_node(node_id).await? {
                *az_counts.entry(node.availability_zone.clone()).or_insert(0) += 1;
            }
        }

        // AZ diversity score
        let unique_azs = az_counts.len();
        let total_azs = topology_manager.get_azs_in_region(region).await?.len();
        let az_score = if total_azs > 0 {
            unique_azs as f64 / total_azs as f64
        } else {
            0.0
        };

        // Size score
        let size_score = self.calculate_size_balance_score(proposed_members.len(), target_size);

        // Need score (how much do we need a new group?)
        let current_groups = allocations.get_region_groups(region).len();
        let total_nodes_in_region = topology_manager.get_node_count_by_region(region).await?;
        let ideal_groups = (total_nodes_in_region as f64 / target_size as f64).ceil() as usize;
        let need_score = if current_groups < ideal_groups {
            1.0
        } else {
            0.3 // Lower score if we already have enough groups
        };

        // Combine scores
        Ok((az_score * 0.4 + size_score * 0.3 + need_score * 0.3).min(1.0))
    }

    /// Find the best nodes to form a new group
    pub async fn select_nodes_for_new_group<G>(
        &self,
        _region: &str,
        available_nodes: &[NodeId],
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        target_size: usize,
        min_size: usize,
    ) -> ConsensusResult<Vec<NodeId>>
    where
        G: Governance + Send + Sync + 'static,
    {
        if available_nodes.len() < min_size {
            return Ok(vec![]);
        }

        // Group nodes by AZ
        let mut nodes_by_az: HashMap<String, Vec<NodeId>> = HashMap::new();
        for node_id in available_nodes {
            if let Some(node) = topology_manager.get_governance_node(node_id).await? {
                nodes_by_az
                    .entry(node.availability_zone.clone())
                    .or_default()
                    .push(node_id.clone());
            }
        }

        // Select nodes to maximize AZ diversity
        let mut selected = Vec::new();
        let mut azs: Vec<_> = nodes_by_az.keys().cloned().collect();

        // Randomize AZ order for fairness
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        azs.shuffle(&mut rng);

        // Round-robin selection from different AZs
        let mut az_index = 0;
        while selected.len() < target_size && selected.len() < available_nodes.len() {
            let az = &azs[az_index % azs.len()];
            if let Some(az_nodes) = nodes_by_az.get_mut(az) {
                if let Some(node_id) = az_nodes.pop() {
                    // Check if this node is suitable (not in too many groups)
                    let current_memberships = allocations.get_node_groups(&node_id).len();
                    if current_memberships < 2 {
                        // Prefer nodes with fewer memberships
                        selected.push(node_id);
                    } else {
                        // Put it back for potential later selection
                        az_nodes.push(node_id);
                    }
                }
            }
            az_index += 1;

            // Break if we've tried all AZs and can't find more suitable nodes
            if az_index >= azs.len() * 2 && selected.len() >= min_size {
                break;
            }
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_az_diversity_scoring() {
        use proven_governance::GovernanceNode;
        use proven_governance_mock::MockGovernance;
        use std::collections::HashSet;

        // Create nodes in different AZs for testing
        let nodes = vec![
            GovernanceNode {
                region: "us-east-1".to_string(),
                availability_zone: "us-east-1a".to_string(),
                origin: "http://127.0.0.1:8000".to_string(),
                public_key: *NodeId::from_seed(0).verifying_key(),
                specializations: HashSet::new(),
            },
            GovernanceNode {
                region: "us-east-1".to_string(),
                availability_zone: "us-east-1a".to_string(),
                origin: "http://127.0.0.1:8001".to_string(),
                public_key: *NodeId::from_seed(1).verifying_key(),
                specializations: HashSet::new(),
            },
            GovernanceNode {
                region: "us-east-1".to_string(),
                availability_zone: "us-east-1b".to_string(),
                origin: "http://127.0.0.1:8002".to_string(),
                public_key: *NodeId::from_seed(2).verifying_key(),
                specializations: HashSet::new(),
            },
        ];

        let governance = std::sync::Arc::new(MockGovernance::new(
            nodes,
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        ));

        let topology_manager =
            crate::topology::TopologyManager::new(governance, NodeId::from_seed(0));

        let scorer = AllocationScorer::new();

        // Create a group with nodes only in one AZ (us-east-1a)
        let group = ConsensusGroup {
            id: ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![NodeId::from_seed(0), NodeId::from_seed(1)]
                .into_iter()
                .collect(),
            state: super::super::GroupState::Active,
        };

        // Score should be higher for adding a node from a new AZ (us-east-1b)
        let score_new_az = scorer
            .calculate_az_diversity_score(&group, "us-east-1b", &topology_manager)
            .await
            .unwrap();
        // Score for same AZ (us-east-1a) should be lower
        let score_same_az = scorer
            .calculate_az_diversity_score(&group, "us-east-1a", &topology_manager)
            .await
            .unwrap();

        println!("Score for new AZ (us-east-1b): {}", score_new_az);
        println!("Score for same AZ (us-east-1a): {}", score_same_az);

        assert!(
            score_new_az > score_same_az,
            "Adding node from new AZ should score higher than same AZ. New AZ: {}, Same AZ: {}",
            score_new_az,
            score_same_az
        );
    }

    #[test]
    fn test_size_balance_scoring() {
        let scorer = AllocationScorer::new();

        let target_size = 5;

        // Perfect size should score 1.0 (e^0 = 1)
        assert_eq!(scorer.calculate_size_balance_score(5, target_size), 1.0);

        // Slight deviation should still score well
        // Deviation of 1: e^(-1/5) = e^(-0.2) ≈ 0.819
        let score_4 = scorer.calculate_size_balance_score(4, target_size);
        assert!(score_4 > 0.8 && score_4 < 0.85);

        let score_6 = scorer.calculate_size_balance_score(6, target_size);
        assert!(score_6 > 0.8 && score_6 < 0.85);

        // Large deviation should score poorly
        // Deviation of 3: e^(-3/5) = e^(-0.6) ≈ 0.549
        let score_2 = scorer.calculate_size_balance_score(2, target_size);
        assert!(score_2 > 0.5 && score_2 < 0.6);

        // Deviation of 4: e^(-4/5) = e^(-0.8) ≈ 0.449
        let score_9 = scorer.calculate_size_balance_score(9, target_size);
        assert!(score_9 > 0.4 && score_9 < 0.5);
    }

    #[test]
    fn test_membership_scoring() {
        let scorer = AllocationScorer::new();
        let mut allocations = GroupAllocations::new();

        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        // Node with no groups should score highest
        assert_eq!(scorer.calculate_membership_score(&node1, &allocations), 1.0);

        // Add node to a group
        let group1_members = vec![node1.clone()].into_iter().collect();
        allocations.create_group("us-east-1".to_string(), group1_members);

        // Node with one group should score lower
        assert!(scorer.calculate_membership_score(&node1, &allocations) < 1.0);
        assert!(scorer.calculate_membership_score(&node1, &allocations) > 0.5);

        // Node with no groups should still score highest
        assert_eq!(scorer.calculate_membership_score(&node2, &allocations), 1.0);
    }
}
