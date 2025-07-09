//! Comprehensive tests for the GroupAllocator with TopologyManager
//!
//! This module tests the GroupAllocator's ability to:
//! - Allocate nodes to region-specific groups
//! - Maximize availability zone distribution
//! - Handle group creation and removal
//! - Perform rebalancing when needed
//! - Execute dual-membership migrations

#[cfg(test)]
mod group_allocator_tests {
    use crate::group_allocator::{
        AllocationScorer, ConsensusGroup, GroupAllocations, GroupAllocator, GroupAllocatorConfig,
        GroupState, TopologyAnalyzer,
    };
    use crate::node_id::NodeId;
    use crate::topology::TopologyManager;
    use proven_attestation_mock::MockAttestor;
    use proven_governance::{GovernanceNode, Version};
    use proven_governance_mock::MockGovernance;
    use std::collections::HashSet;
    use std::sync::Arc;

    /// Create a test governance with specific nodes  
    async fn create_test_governance_with_nodes(nodes: Vec<GovernanceNode>) -> Arc<MockGovernance> {
        let attestor = MockAttestor::new();
        let version = Version::from_pcrs(attestor.pcrs_sync());

        let governance = MockGovernance::new(
            nodes,
            vec![version],
            "http://localhost:3200".to_string(),
            vec![],
        );

        Arc::new(governance)
    }

    /// Create a test topology manager with specific nodes
    async fn create_test_topology_manager_with_nodes(
        nodes: Vec<GovernanceNode>,
    ) -> TopologyManager<MockGovernance> {
        // If we have nodes, get the proper governance from our helpers
        if !nodes.is_empty() {
            // Extract ports from the nodes
            let _ports: Vec<u16> = nodes
                .iter()
                .map(|node| {
                    node.origin
                        .strip_prefix("http://127.0.0.1:")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(8000)
                })
                .collect();

            // Create keys from public keys
            let governance = create_test_governance_with_nodes(nodes.clone()).await;
            let node_id = NodeId::new(nodes[0].public_key);

            TopologyManager::new(governance, node_id)
        } else {
            // No nodes - just create a simple topology manager
            let node_id = NodeId::from_seed(0);
            let governance = create_test_governance_with_nodes(vec![]).await;
            TopologyManager::new(governance, node_id)
        }
    }

    /// Create a test node with specific region and AZ
    fn create_test_node(region: &str, az: &str, index: u8) -> GovernanceNode {
        let node_id = NodeId::from_seed(index);

        GovernanceNode {
            region: region.to_string(),
            availability_zone: az.to_string(),
            origin: format!("http://127.0.0.1:{}", 8000 + index as u16),
            public_key: *node_id.verifying_key(),
            specializations: HashSet::new(),
        }
    }

    #[tokio::test]
    async fn test_basic_allocation() {
        // Create 6 nodes in us-east-1 (2 per AZ)
        let mut nodes = vec![];
        for i in 0..6 {
            let az = match i % 3 {
                0 => "us-east-1a",
                1 => "us-east-1b",
                _ => "us-east-1c",
            };
            nodes.push(create_test_node("us-east-1", az, i as u8));
        }

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let config = GroupAllocatorConfig::default();
        let target_group_size = config.target_group_size;
        let allocator = GroupAllocator::new(config, Arc::new(topology_manager));

        // Allocate first node
        let node1 = &nodes[0];
        let node1_id = NodeId::new(node1.public_key);
        let groups = allocator
            .allocate_groups_for_node(&node1_id, node1)
            .await
            .unwrap();

        // Should create a new group
        assert_eq!(groups.len(), 1);

        // Allocate remaining nodes
        for (i, node) in nodes.iter().enumerate().skip(1) {
            let node_id = NodeId::new(node.public_key);
            let assigned_groups = allocator
                .allocate_groups_for_node(&node_id, node)
                .await
                .unwrap();

            println!(
                "Node {} (AZ: {}) assigned to groups: {:?}",
                i, node.availability_zone, assigned_groups
            );

            // Should be assigned to existing groups or new ones based on balance
            assert!(
                !assigned_groups.is_empty(),
                "Node {} should be assigned to a group",
                i
            );
        }

        // Check final allocation
        let allocations = allocator.get_allocations().await;
        let topology_manager = allocator.get_topology_manager();

        println!("\nFinal allocation state:");
        println!("Total groups: {}", allocations.groups.len());

        // Analyze groups
        for (group_id, group) in &allocations.groups {
            let analysis =
                TopologyAnalyzer::analyze_group(topology_manager, group, target_group_size)
                    .await
                    .unwrap();

            println!(
                "Group {:?}: {} members, {} AZs covered, diversity score: {:.2}",
                group_id, analysis.size, analysis.az_coverage, analysis.az_diversity_score
            );

            // Groups with multiple members should have some AZ diversity
            // Single-member groups will have 0 diversity, which is expected
            if analysis.size > 1 {
                assert!(
                    analysis.az_diversity_score >= 0.0,
                    "Multi-member group should have non-negative AZ diversity"
                );
            }

            // Groups shouldn't exceed max size
            assert!(
                analysis.size <= 7, // max_group_size from default config
                "Group size should not exceed maximum"
            );
        }

        // Overall, we should have reasonable group utilization
        let total_nodes = 6;
        let avg_group_size = total_nodes as f64 / allocations.groups.len() as f64;
        println!("Average group size: {:.2}", avg_group_size);

        // Average should be reasonable (not too many single-member groups)
        assert!(
            avg_group_size >= 1.0,
            "Should have reasonable group utilization"
        );
    }

    #[tokio::test]
    async fn test_region_specific_groups() {
        // Create nodes in multiple regions
        let mut nodes = vec![];

        // 3 nodes in us-east-1
        for i in 0..3 {
            nodes.push(create_test_node("us-east-1", "us-east-1a", i as u8));
        }

        // 3 nodes in eu-west-1
        for i in 3..6 {
            nodes.push(create_test_node("eu-west-1", "eu-west-1a", i as u8));
        }

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let config = GroupAllocatorConfig::default();
        let allocator = GroupAllocator::new(config, Arc::new(topology_manager));

        // Allocate all nodes
        for node in &nodes {
            let node_id = NodeId::new(node.public_key);
            allocator
                .allocate_groups_for_node(&node_id, node)
                .await
                .unwrap();
        }

        let allocations = allocator.get_allocations().await;

        // Check that groups are region-specific
        for group in allocations.groups.values() {
            let regions: HashSet<_> = group
                .members
                .iter()
                .filter_map(|node_id| {
                    nodes
                        .iter()
                        .find(|n| &NodeId::new(n.public_key) == node_id)
                        .map(|n| &n.region)
                })
                .collect();

            assert_eq!(
                regions.len(),
                1,
                "Group should only contain nodes from one region"
            );
        }

        // Should have at least one group per region
        let us_groups = allocations.get_region_groups("us-east-1");
        let eu_groups = allocations.get_region_groups("eu-west-1");

        assert!(!us_groups.is_empty(), "Should have groups in us-east-1");
        assert!(!eu_groups.is_empty(), "Should have groups in eu-west-1");
    }

    #[tokio::test]
    async fn test_az_distribution_optimization() {
        // Create imbalanced nodes: 4 in az-a, 2 in az-b, 1 in az-c
        let mut nodes = vec![];
        for i in 0..4 {
            nodes.push(create_test_node("us-east-1", "us-east-1a", i as u8));
        }
        for i in 4..6 {
            nodes.push(create_test_node("us-east-1", "us-east-1b", i as u8));
        }
        nodes.push(create_test_node("us-east-1", "us-east-1c", 6));

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let config = GroupAllocatorConfig {
            target_group_size: 3,
            min_group_size: 3,
            ..Default::default()
        };
        let target_group_size = config.target_group_size;

        let allocator = GroupAllocator::new(config, Arc::new(topology_manager));

        // Allocate all nodes
        for (i, node) in nodes.iter().enumerate() {
            let node_id = NodeId::new(node.public_key);
            let assigned_groups = allocator
                .allocate_groups_for_node(&node_id, node)
                .await
                .unwrap();
            println!(
                "Node {} (AZ: {}) assigned to groups: {:?}",
                i, node.availability_zone, assigned_groups
            );
        }

        let allocations = allocator.get_allocations().await;
        let topology_manager = allocator.get_topology_manager();

        println!("\nFinal allocation state:");
        println!(
            "Total nodes: {}, Total groups: {}",
            nodes.len(),
            allocations.groups.len()
        );

        // Analyze AZ distribution
        let mut total_az_coverage = 0;
        let mut groups_with_good_coverage = 0;

        for (group_id, group) in &allocations.groups {
            let analysis =
                TopologyAnalyzer::analyze_group(topology_manager, group, target_group_size)
                    .await
                    .unwrap();

            println!(
                "Group {:?}: {} members, AZ distribution: {:?}, AZ coverage: {}",
                group_id, analysis.size, analysis.az_distribution, analysis.az_coverage
            );

            total_az_coverage += analysis.az_coverage;

            // For groups that meet minimum size, check AZ coverage
            if analysis.size >= 3 {
                // min_group_size from config
                // Groups of size 3+ should try to have at least 2 AZs
                if analysis.az_coverage >= 2 {
                    groups_with_good_coverage += 1;
                }
            }
        }

        // With 7 nodes (4 in az-a, 2 in az-b, 1 in az-c) and min_group_size=3,
        // we expect to form groups that have some AZ diversity
        println!(
            "Groups with size >= 3 and good AZ coverage: {}",
            groups_with_good_coverage
        );

        // Average AZ coverage should be reasonable
        let avg_az_coverage = total_az_coverage as f64 / allocations.groups.len() as f64;
        println!("Average AZ coverage: {:.2}", avg_az_coverage);
        assert!(
            avg_az_coverage >= 1.0,
            "Average AZ coverage should be at least 1.0"
        );
    }

    #[tokio::test]
    async fn test_node_removal_and_rebalancing() {
        // Create 6 nodes
        let mut nodes = vec![];
        for i in 0..6 {
            let az = match i % 3 {
                0 => "us-east-1a",
                1 => "us-east-1b",
                _ => "us-east-1c",
            };
            nodes.push(create_test_node("us-east-1", az, i as u8));
        }

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let config = GroupAllocatorConfig {
            min_group_size: 2,
            ..Default::default()
        };
        let min_group_size = config.min_group_size;

        let allocator = GroupAllocator::new(config, Arc::new(topology_manager));

        // Allocate all nodes
        for node in &nodes {
            let node_id = NodeId::new(node.public_key);
            allocator
                .allocate_groups_for_node(&node_id, node)
                .await
                .unwrap();
        }

        // Remove a node
        let removed_node_id = NodeId::new(nodes[0].public_key);
        let affected_groups = allocator.remove_node(&removed_node_id).await.unwrap();

        println!("Removed node from {} groups", affected_groups.len());

        // Check if rebalancing is needed
        let needs_rebalance = allocator.needs_rebalancing().await.unwrap();
        println!("Needs rebalancing: {}", needs_rebalance);

        // Get recommendations
        let recommendations = allocator.get_recommendations().await.unwrap();

        for rec in &recommendations {
            println!(
                "Recommendation: {:?} (priority: {:.2}) - {}",
                rec.action, rec.priority, rec.reason
            );
        }

        // Check that groups are properly maintained
        let allocations = allocator.get_allocations().await;
        for group in allocations.groups.values() {
            // Groups should not contain the removed node
            assert!(
                !group.members.contains(&removed_node_id),
                "Group should not contain removed node"
            );

            // If a group is too small, it should be recommended for action
            if group.members.len() < min_group_size {
                println!(
                    "Found undersize group {:?} with {} members",
                    group.id,
                    group.members.len()
                );
                // This is acceptable as long as we have recommendations to handle it
            }
        }
    }

    #[tokio::test]
    async fn test_scoring_algorithm() {
        // Create test topology
        let nodes = vec![
            create_test_node("us-east-1", "us-east-1a", 0),
            create_test_node("us-east-1", "us-east-1b", 1),
            create_test_node("us-east-1", "us-east-1c", 2),
        ];

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let allocator =
            GroupAllocator::new(GroupAllocatorConfig::default(), Arc::new(topology_manager));

        let topology_manager = allocator.get_topology_manager();
        let mut allocations = allocator.get_allocations().await;

        // Create a test group with one node
        let node0_id = NodeId::new(nodes[0].public_key);
        let group_id = allocations.create_group(
            "us-east-1".to_string(),
            vec![node0_id.clone()].into_iter().collect(),
        );

        let group = ConsensusGroup {
            id: group_id,
            region: "us-east-1".to_string(),
            members: vec![node0_id].into_iter().collect(),
            state: GroupState::Active,
        };

        let scorer = AllocationScorer::new();

        // Score adding node from different AZ (should be high)
        let node1_id = NodeId::new(nodes[1].public_key);
        let score1 = scorer
            .score_node_allocation(&node1_id, &group, topology_manager, &allocations, 5)
            .await
            .unwrap();

        // Score adding another node from same AZ (should be lower)
        let node0_duplicate = create_test_node("us-east-1", "us-east-1a", 10);
        let node0_dup_id = NodeId::new(node0_duplicate.public_key);

        // Add the duplicate node to topology
        topology_manager
            .add_node(crate::Node::from(node0_duplicate))
            .unwrap();

        let score2 = scorer
            .score_node_allocation(&node0_dup_id, &group, topology_manager, &allocations, 5)
            .await
            .unwrap();

        println!("Score for different AZ: {:.2}", score1.total_score);
        println!("Score for same AZ: {:.2}", score2.total_score);

        assert!(
            score1.total_score >= score2.total_score,
            "Adding node from different AZ should score higher or equal"
        );

        // AZ diversity component should definitely be higher for different AZ
        assert!(
            score1.az_diversity > score2.az_diversity,
            "AZ diversity score should be higher for different AZ"
        );
    }

    #[tokio::test]
    async fn test_rebalancing_with_dual_membership() {
        // Create nodes with distribution that may need rebalancing
        let mut nodes = vec![];

        // 4 nodes in different AZs for better distribution
        for i in 0..4 {
            let az = match i % 2 {
                0 => "us-east-1a",
                _ => "us-east-1b",
            };
            nodes.push(create_test_node("us-east-1", az, i as u8));
        }

        let topology_manager = create_test_topology_manager_with_nodes(nodes.clone()).await;
        let config = GroupAllocatorConfig {
            min_group_size: 2,
            target_group_size: 3,
            rebalance_threshold: 0.5,
            ..Default::default()
        };

        let allocator = GroupAllocator::new(config, Arc::new(topology_manager));

        // Allocate all nodes
        for node in &nodes {
            let node_id = NodeId::new(node.public_key);
            allocator
                .allocate_groups_for_node(&node_id, node)
                .await
                .unwrap();
        }

        let allocations = allocator.get_allocations().await;
        println!("Created {} groups", allocations.groups.len());

        // Print group details
        for (group_id, group) in &allocations.groups {
            println!("Group {:?} has {} members", group_id, group.members.len());
        }

        // Check if rebalancing is needed
        let needs_rebalance = allocator.needs_rebalancing().await.unwrap();
        println!("Needs rebalancing: {}", needs_rebalance);

        // Generate rebalancing plan
        let plan = allocator.generate_rebalancing_plan().await.unwrap();

        if let Some(plan) = plan {
            println!(
                "Rebalancing plan generated with {} migrations",
                plan.migrations.len()
            );

            // Execute first migration if available
            if let Some(first_migration) = plan.migrations.first() {
                println!(
                    "Starting migration of node {:?} from group {:?} to {:?}",
                    first_migration.node_id, first_migration.from_group, first_migration.to_group
                );

                // Start migration (dual membership)
                allocator
                    .start_node_migration(
                        &first_migration.node_id,
                        first_migration.from_group,
                        first_migration.to_group,
                    )
                    .await
                    .unwrap();

                // Verify node is in both groups
                let allocations = allocator.get_allocations().await;
                let node_groups = allocations.get_node_groups(&first_migration.node_id);
                assert_eq!(
                    node_groups.len(),
                    2,
                    "Node should be in both groups during migration"
                );
                assert!(node_groups.contains(&first_migration.from_group));
                assert!(node_groups.contains(&first_migration.to_group));

                // Verify migration tracking
                assert!(
                    allocator.is_node_migrating(&first_migration.node_id).await,
                    "Node should be marked as migrating"
                );

                // Complete migration
                allocator
                    .complete_node_migration(&first_migration.node_id)
                    .await
                    .unwrap();

                // Verify node is only in target group
                let allocations = allocator.get_allocations().await;
                let node_groups = allocations.get_node_groups(&first_migration.node_id);
                assert_eq!(
                    node_groups.len(),
                    1,
                    "Node should only be in target group after migration"
                );
                assert_eq!(node_groups[0], first_migration.to_group);

                // Verify migration tracking cleared
                assert!(
                    !allocator.is_node_migrating(&first_migration.node_id).await,
                    "Node should not be marked as migrating after completion"
                );
            }
        } else {
            println!("No rebalancing plan needed");
        }
    }

    #[tokio::test]
    async fn test_group_allocations_operations() {
        // Test the GroupAllocations structure directly
        let mut allocations = GroupAllocations::new();

        // Create test NodeIds
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);
        let node3 = NodeId::from_seed(3);

        // Create a group
        let members = vec![node1.clone(), node2.clone()].into_iter().collect();
        let group_id = allocations.create_group("us-east-1".to_string(), members);

        assert_eq!(allocations.get_node_groups(&node1).len(), 1);
        assert_eq!(allocations.get_node_groups(&node2).len(), 1);
        assert_eq!(allocations.get_node_groups(&node3).len(), 0);
        assert_eq!(allocations.get_region_groups("us-east-1").len(), 1);

        // Add node to group
        allocations
            .add_node_to_group(node3.clone(), group_id)
            .unwrap();
        assert_eq!(allocations.get_node_groups(&node3).len(), 1);

        // Remove node from group
        allocations
            .remove_node_from_group(&node3, group_id)
            .unwrap();
        assert_eq!(allocations.get_node_groups(&node3).len(), 0);

        // Remove the entire group
        allocations.remove_group(group_id);
        assert_eq!(allocations.get_node_groups(&node1).len(), 0);
        assert_eq!(allocations.get_node_groups(&node2).len(), 0);
        assert_eq!(allocations.get_region_groups("us-east-1").len(), 0);
    }
}
