//! Topology analysis utilities for group allocation
//!
//! This module provides utilities for analyzing cluster topology to make
//! intelligent allocation decisions based on region and availability zone distribution.

use super::allocator::{ConsensusGroup, GroupAllocations};
use crate::allocation::ConsensusGroupId;
use crate::error::ConsensusResult;
use crate::node_id::NodeId;
use crate::topology::TopologyManager;
use proven_governance::Governance;
use std::collections::HashMap;

/// Analysis result for a region's topology
#[derive(Debug, Clone)]
pub struct RegionAnalysis {
    /// Region name
    pub region: String,
    /// Total nodes in the region
    pub total_nodes: usize,
    /// Number of availability zones
    pub az_count: usize,
    /// Node distribution across AZs
    pub az_distribution: HashMap<String, usize>,
    /// Current number of groups
    pub current_groups: usize,
    /// Recommended number of groups
    pub recommended_groups: usize,
    /// AZ balance score (0.0 = perfectly balanced, 1.0 = completely imbalanced)
    pub az_balance_score: f64,
}

/// Analysis result for a single group
#[derive(Debug, Clone)]
pub struct GroupAnalysis {
    /// Group ID
    pub group_id: ConsensusGroupId,
    /// Region
    pub region: String,
    /// Current size
    pub size: usize,
    /// AZ coverage (number of unique AZs)
    pub az_coverage: usize,
    /// AZ distribution
    pub az_distribution: HashMap<String, usize>,
    /// AZ diversity score (0.0 = all in one AZ, 1.0 = perfectly distributed)
    pub az_diversity_score: f64,
    /// Size balance score relative to target
    pub size_balance_score: f64,
}

/// Analyzes topology for allocation decisions
pub struct TopologyAnalyzer;

impl TopologyAnalyzer {
    /// Analyze a region's topology
    pub async fn analyze_region<G>(
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        region: &str,
        target_group_size: usize,
    ) -> ConsensusResult<RegionAnalysis>
    where
        G: Governance + Send + Sync + 'static,
    {
        let total_nodes = topology_manager.get_node_count_by_region(region).await?;
        let azs = topology_manager.get_azs_in_region(region).await?;
        let az_count = azs.len();

        // Get AZ distribution
        let mut az_distribution = HashMap::new();
        for az in &azs {
            let count = topology_manager
                .get_node_count_by_region_az(region, Some(az))
                .await?;
            az_distribution.insert(az.clone(), count);
        }

        // Count current groups
        let current_groups = allocations.get_region_groups(region).len();

        // Calculate recommended groups
        let recommended_groups = if total_nodes == 0 {
            0
        } else {
            // At least 1 group, but try to keep groups around target size
            ((total_nodes as f64 / target_group_size as f64).ceil() as usize).max(1)
        };

        // Calculate AZ balance score
        let az_balance_score = Self::calculate_az_balance_score(&az_distribution);

        Ok(RegionAnalysis {
            region: region.to_string(),
            total_nodes,
            az_count,
            az_distribution,
            current_groups,
            recommended_groups,
            az_balance_score,
        })
    }

    /// Analyze a group's topology
    pub async fn analyze_group<G>(
        topology_manager: &TopologyManager<G>,
        group: &ConsensusGroup,
        target_size: usize,
    ) -> ConsensusResult<GroupAnalysis>
    where
        G: Governance + Send + Sync + 'static,
    {
        let size = group.members.len();

        // Calculate AZ distribution
        let mut az_distribution: HashMap<String, usize> = HashMap::new();
        for node_id in &group.members {
            if let Some(node) = topology_manager.get_governance_node(node_id).await? {
                *az_distribution
                    .entry(node.availability_zone.clone())
                    .or_insert(0) += 1;
            }
        }

        let az_coverage = az_distribution.len();
        let az_diversity_score = Self::calculate_az_diversity_score(&az_distribution, size);
        let size_balance_score = Self::calculate_size_balance_score(size, target_size);

        Ok(GroupAnalysis {
            group_id: group.id,
            region: group.region.clone(),
            size,
            az_coverage,
            az_distribution,
            az_diversity_score,
            size_balance_score,
        })
    }

    /// Calculate how well balanced nodes are across AZs in a region
    /// Returns 0.0 for perfect balance, approaching 1.0 for complete imbalance
    fn calculate_az_balance_score(az_distribution: &HashMap<String, usize>) -> f64 {
        if az_distribution.is_empty() {
            return 0.0;
        }

        let total_nodes: usize = az_distribution.values().sum();
        if total_nodes == 0 {
            return 0.0;
        }

        let ideal_per_az = total_nodes as f64 / az_distribution.len() as f64;
        let mut total_deviation = 0.0;

        for &count in az_distribution.values() {
            let deviation = (count as f64 - ideal_per_az).abs();
            total_deviation += deviation;
        }

        // Normalize to 0-1 range
        total_deviation / (total_nodes as f64)
    }

    /// Calculate AZ diversity score for a group
    /// Returns 1.0 for perfect distribution across AZs, 0.0 if all nodes in one AZ
    fn calculate_az_diversity_score(
        az_distribution: &HashMap<String, usize>,
        total_nodes: usize,
    ) -> f64 {
        if total_nodes <= 1 || az_distribution.len() <= 1 {
            return 0.0;
        }

        // Shannon entropy normalized to 0-1
        let mut entropy = 0.0;
        for &count in az_distribution.values() {
            if count > 0 {
                let probability = count as f64 / total_nodes as f64;
                entropy -= probability * probability.log2();
            }
        }

        // Normalize by maximum possible entropy
        let max_entropy = (az_distribution.len() as f64).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy
        } else {
            0.0
        }
    }

    /// Calculate size balance score
    /// Returns 0.0 for perfect size, approaching 1.0 as size deviates from target
    fn calculate_size_balance_score(size: usize, target_size: usize) -> f64 {
        if target_size == 0 {
            return 1.0;
        }

        let deviation = (size as i32 - target_size as i32).abs() as f64;
        let normalized_deviation = deviation / target_size as f64;

        // Cap at 1.0
        normalized_deviation.min(1.0)
    }

    /// Find the best AZ to add a node to improve diversity
    pub async fn find_best_az_for_group<G>(
        topology_manager: &TopologyManager<G>,
        group: &ConsensusGroup,
        available_azs: &[String],
    ) -> ConsensusResult<Option<String>>
    where
        G: Governance + Send + Sync + 'static,
    {
        if available_azs.is_empty() {
            return Ok(None);
        }

        // Count current AZ distribution in the group
        let mut az_counts: HashMap<String, usize> = HashMap::new();
        for node_id in &group.members {
            if let Some(node) = topology_manager.get_governance_node(node_id).await? {
                *az_counts.entry(node.availability_zone.clone()).or_insert(0) += 1;
            }
        }

        // Find AZ with lowest representation
        let mut best_az = None;
        let mut min_count = usize::MAX;

        for az in available_azs {
            let count = az_counts.get(az).copied().unwrap_or(0);
            if count < min_count {
                min_count = count;
                best_az = Some(az.clone());
            }
        }

        Ok(best_az)
    }

    /// Calculate the impact of adding a node to a group
    pub async fn calculate_add_node_impact<G>(
        topology_manager: &TopologyManager<G>,
        group: &ConsensusGroup,
        node_id: &NodeId,
        target_size: usize,
    ) -> ConsensusResult<GroupImpact>
    where
        G: Governance + Send + Sync + 'static,
    {
        let node = match topology_manager.get_governance_node(node_id).await? {
            Some(n) => n,
            None => return Ok(GroupImpact::default()),
        };

        // Current state
        let current_analysis = Self::analyze_group(topology_manager, group, target_size).await?;

        // Simulate adding the node
        let mut new_az_distribution = current_analysis.az_distribution.clone();
        *new_az_distribution
            .entry(node.availability_zone.clone())
            .or_insert(0) += 1;

        let new_size = current_analysis.size + 1;
        let new_az_diversity = Self::calculate_az_diversity_score(&new_az_distribution, new_size);
        let new_size_balance = Self::calculate_size_balance_score(new_size, target_size);

        Ok(GroupImpact {
            size_change: 1,
            az_diversity_change: new_az_diversity - current_analysis.az_diversity_score,
            size_balance_change: new_size_balance - current_analysis.size_balance_score,
            new_az_added: !current_analysis
                .az_distribution
                .contains_key(&node.availability_zone),
        })
    }

    /// Find nodes that would improve a group's AZ diversity
    pub async fn find_nodes_for_az_improvement<G>(
        topology_manager: &TopologyManager<G>,
        allocations: &GroupAllocations,
        group: &ConsensusGroup,
        max_concurrent_groups: usize,
    ) -> ConsensusResult<Vec<(NodeId, f64)>>
    where
        G: Governance + Send + Sync + 'static,
    {
        // Get current AZ distribution
        let analysis = Self::analyze_group(topology_manager, group, 5).await?; // Use default target

        // Find underrepresented AZs
        let region_azs = topology_manager.get_azs_in_region(&group.region).await?;
        let mut underrepresented_azs = Vec::new();

        for az in region_azs {
            let count = analysis.az_distribution.get(&az).copied().unwrap_or(0);
            if count == 0 || count < analysis.size / analysis.az_coverage {
                underrepresented_azs.push(az);
            }
        }

        // Find nodes in underrepresented AZs that aren't at max groups
        let mut candidates = Vec::new();
        let region_nodes = topology_manager.get_nodes_by_region(&group.region).await?;

        for node in region_nodes {
            let node_id = NodeId::new(node.public_key());
            if let Some(governance_node) = topology_manager.get_governance_node(&node_id).await? {
                let current_groups = allocations.get_node_groups(&node_id).len();

                if current_groups < max_concurrent_groups
                    && underrepresented_azs.contains(&governance_node.availability_zone)
                    && !group.members.contains(&node_id)
                {
                    // Calculate improvement score
                    let impact = Self::calculate_add_node_impact(
                        topology_manager,
                        group,
                        &node_id,
                        analysis.size,
                    )
                    .await?;
                    let score = impact.az_diversity_change;
                    candidates.push((node_id.clone(), score));
                }
            }
        }

        // Sort by improvement score (descending)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        Ok(candidates)
    }
}

/// Impact of adding/removing a node from a group
#[derive(Debug, Clone, Default)]
pub struct GroupImpact {
    /// Change in group size
    pub size_change: i32,
    /// Change in AZ diversity score
    pub az_diversity_change: f64,
    /// Change in size balance score
    pub size_balance_change: f64,
    /// Whether a new AZ is added
    pub new_az_added: bool,
}

/// Recommendations for group operations
#[derive(Debug, Clone)]
pub struct GroupRecommendation {
    /// Type of recommendation
    pub action: RecommendedAction,
    /// Priority (higher = more urgent)
    pub priority: f64,
    /// Reason for the recommendation
    pub reason: String,
}

/// Recommended actions for groups
#[derive(Debug, Clone)]
pub enum RecommendedAction {
    /// Create a new group in a region
    CreateGroup {
        /// The region where the group should be created
        region: String,
    },
    /// Remove an underutilized group
    RemoveGroup {
        /// The ID of the group to remove
        group_id: ConsensusGroupId,
    },
    /// Add nodes to a group
    AddNodes {
        /// The ID of the group to add nodes to
        group_id: ConsensusGroupId,
        /// The IDs of nodes to add
        node_ids: Vec<NodeId>,
    },
    /// Remove nodes from a group
    RemoveNodes {
        /// The ID of the group to remove nodes from
        group_id: ConsensusGroupId,
        /// The IDs of nodes to remove
        node_ids: Vec<NodeId>,
    },
    /// Rebalance nodes between groups
    RebalanceNodes {
        /// The source group to move nodes from
        from_group: ConsensusGroupId,
        /// The target group to move nodes to
        to_group: ConsensusGroupId,
        /// The IDs of nodes to move
        node_ids: Vec<NodeId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_az_balance_score() {
        let mut distribution = HashMap::new();

        // Perfect balance: 3 nodes in each of 3 AZs
        distribution.insert("az1".to_string(), 3);
        distribution.insert("az2".to_string(), 3);
        distribution.insert("az3".to_string(), 3);
        let score = TopologyAnalyzer::calculate_az_balance_score(&distribution);
        assert!(score < 0.01); // Should be close to 0

        // Complete imbalance: all nodes in one AZ
        distribution.clear();
        distribution.insert("az1".to_string(), 9);
        distribution.insert("az2".to_string(), 0);
        distribution.insert("az3".to_string(), 0);
        let score = TopologyAnalyzer::calculate_az_balance_score(&distribution);
        assert!(score > 0.5); // Should be high

        // Slight imbalance
        distribution.clear();
        distribution.insert("az1".to_string(), 4);
        distribution.insert("az2".to_string(), 3);
        distribution.insert("az3".to_string(), 2);
        let score = TopologyAnalyzer::calculate_az_balance_score(&distribution);
        assert!(score > 0.0 && score < 0.3); // Should be moderate
    }

    #[test]
    fn test_az_diversity_score() {
        let mut distribution = HashMap::new();

        // All nodes in one AZ
        distribution.insert("az1".to_string(), 5);
        let score = TopologyAnalyzer::calculate_az_diversity_score(&distribution, 5);
        assert_eq!(score, 0.0);

        // Perfect distribution across 3 AZs
        distribution.clear();
        distribution.insert("az1".to_string(), 2);
        distribution.insert("az2".to_string(), 2);
        distribution.insert("az3".to_string(), 2);
        let score = TopologyAnalyzer::calculate_az_diversity_score(&distribution, 6);
        assert!(score > 0.9); // Should be close to 1.0
    }

    #[tokio::test]
    async fn test_find_best_az() {
        use crate::test_helpers::create_test_topology_manager;

        let topology_manager = create_test_topology_manager().await;

        // Create a group with nodes only in az-a and az-b
        let group = ConsensusGroup {
            id: ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![NodeId::from_seed(0), NodeId::from_seed(1)]
                .into_iter()
                .collect(),
            state: crate::group_allocator::GroupState::Active,
        };

        let available_azs = vec![
            "us-east-1a".to_string(),
            "us-east-1b".to_string(),
            "us-east-1c".to_string(),
        ];

        let best_az =
            TopologyAnalyzer::find_best_az_for_group(&topology_manager, &group, &available_azs)
                .await
                .unwrap();
        assert!(best_az.is_some()); // Should pick one of the available AZs
    }
}
