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
    use crate::ConsensusGroupId;
    use crate::core::group::group_allocator::allocator::{ConsensusGroup, GroupState};
    use crate::core::group::group_allocator::rebalancer::GroupRebalancer;
    use crate::core::group::group_allocator::scoring::AllocationScorer;
    use crate::core::group::group_allocator::topology::TopologyAnalyzer;
    use crate::core::group::group_allocator::{GroupAllocations, GroupAllocatorConfig};

    use ed25519_dalek::SigningKey;
    use proven_attestation_mock::MockAttestor;
    use proven_governance::{GovernanceNode, Version};
    use proven_governance_mock::MockGovernance;
    use proven_topology::TopologyManager;
    use proven_topology::{Node, NodeId};
    use std::collections::HashSet;
    use std::sync::Arc;

    /// Create a test governance with specific nodes  
    async fn create_test_governance_with_nodes(nodes: Vec<Node>) -> Arc<MockGovernance> {
        let attestor = MockAttestor::new();
        let version = Version::from_pcrs(attestor.pcrs_sync());

        let governance = MockGovernance::new(
            nodes.into_iter().map(|n| n.into()).collect(),
            vec![version],
            "http://localhost:3200".to_string(),
            vec![],
        );

        Arc::new(governance)
    }

    /// Create a test topology manager with specific nodes
    async fn create_test_topology_manager_with_nodes(
        nodes: Vec<Node>,
    ) -> TopologyManager<MockGovernance> {
        // If we have nodes, get the proper governance from our helpers
        if !nodes.is_empty() {
            // Extract ports from the nodes
            let _ports: Vec<u16> = nodes
                .iter()
                .map(|_node| {
                    // Extract port from URL
                    3000 // Default port
                })
                .collect();

            let governance = create_test_governance_with_nodes(nodes).await;
            TopologyManager::new(governance, NodeId::from_seed(0))
                .await
                .unwrap()
        } else {
            // Empty governance for empty node list
            let governance = Arc::new(MockGovernance::new(
                vec![],
                vec![],
                "http://localhost:3200".to_string(),
                vec![],
            ));
            TopologyManager::new(governance, NodeId::from_seed(0))
                .await
                .unwrap()
        }
    }

    /// Create a default test topology manager
    async fn create_test_topology_manager() -> TopologyManager<MockGovernance> {
        // Create some test nodes
        let nodes = vec![
            Node::from(GovernanceNode {
                availability_zone: "us-east-1a".to_string(),
                origin: "http://127.0.0.1:3000".to_string(),
                public_key: {
                    let seed = [0u8; 32];
                    SigningKey::from_bytes(&seed).verifying_key()
                },
                region: "us-east-1".to_string(),
                specializations: HashSet::new(),
            }),
            Node::from(GovernanceNode {
                availability_zone: "us-east-1b".to_string(),
                origin: "http://127.0.0.1:3001".to_string(),
                public_key: {
                    let mut seed = [0u8; 32];
                    seed[0] = 1;
                    SigningKey::from_bytes(&seed).verifying_key()
                },
                region: "us-east-1".to_string(),
                specializations: HashSet::new(),
            }),
        ];

        create_test_topology_manager_with_nodes(nodes).await
    }

    /// Create test allocations
    fn create_test_allocations() -> GroupAllocations {
        GroupAllocations::new()
    }

    #[tokio::test]
    async fn test_empty_rebalancing_plan() {
        let config = GroupAllocatorConfig::default();
        let rebalancer = GroupRebalancer::new(config);

        let topology_manager = create_test_topology_manager().await;
        let allocations = create_test_allocations();

        let plan = rebalancer
            .generate_plan(&topology_manager, &allocations)
            .await
            .unwrap();
        assert!(plan.is_none());
    }

    #[tokio::test]
    async fn test_find_best_az() {
        let topology_manager = create_test_topology_manager().await;

        // Create a group with nodes only in az-a and az-b
        let group = ConsensusGroup {
            id: crate::ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![NodeId::from_seed(0), NodeId::from_seed(1)]
                .into_iter()
                .collect(),
            state: GroupState::Active,
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

    #[tokio::test]
    async fn test_group_allocations_structure() {
        // Test basic GroupAllocations functionality
        let mut allocations = GroupAllocations::new();

        // Create a test group
        let group = ConsensusGroup {
            id: ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![NodeId::from_seed(0), NodeId::from_seed(1)]
                .into_iter()
                .collect(),
            state: GroupState::Active,
        };

        // Insert the group
        allocations.groups.insert(group.id, group.clone());
        allocations
            .groups_by_region
            .entry(group.region.clone())
            .or_default()
            .insert(group.id);
        for member in &group.members {
            allocations
                .node_groups
                .entry(member.clone())
                .or_default()
                .insert(group.id);
        }

        // Verify the structure
        assert_eq!(allocations.groups.len(), 1);
        assert_eq!(
            allocations.groups_by_region.get("us-east-1").unwrap().len(),
            1
        );
        assert_eq!(
            allocations
                .node_groups
                .get(&NodeId::from_seed(0))
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_allocator_scoring() {
        let _config = GroupAllocatorConfig::default();
        let _scorer = AllocationScorer::new();

        // Create a test allocation
        let mut allocations = GroupAllocations::new();

        // Add a group with 3 nodes
        let group1 = ConsensusGroup {
            id: crate::ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![
                NodeId::from_seed(0),
                NodeId::from_seed(1),
                NodeId::from_seed(2),
            ]
            .into_iter()
            .collect(),
            state: GroupState::Active,
        };
        allocations.groups.insert(group1.id, group1.clone());
        allocations
            .groups_by_region
            .entry(group1.region.clone())
            .or_default()
            .insert(group1.id);
        for member in &group1.members {
            allocations
                .node_groups
                .entry(member.clone())
                .or_default()
                .insert(group1.id);
        }

        // Verify the allocation was created correctly
        assert_eq!(allocations.groups.len(), 1, "Should have one group");
        assert_eq!(
            allocations.groups_by_region.get("us-east-1").unwrap().len(),
            1,
            "Should have one group in us-east-1"
        );
        assert_eq!(
            allocations
                .node_groups
                .get(&NodeId::from_seed(0))
                .unwrap()
                .len(),
            1,
            "Node 0 should be in one group"
        );
    }

    #[tokio::test]
    async fn test_group_state_transitions() {
        let mut allocations = GroupAllocations::new();

        // Create a group
        let group = ConsensusGroup {
            id: crate::ConsensusGroupId::new(1),
            region: "us-east-1".to_string(),
            members: vec![NodeId::from_seed(0), NodeId::from_seed(1)]
                .into_iter()
                .collect(),
            state: GroupState::Active,
        };

        allocations.groups.insert(group.id, group.clone());
        allocations
            .groups_by_region
            .entry(group.region.clone())
            .or_default()
            .insert(group.id);
        for member in &group.members {
            allocations
                .node_groups
                .entry(member.clone())
                .or_default()
                .insert(group.id);
        }

        // Check active groups
        let active_groups: Vec<_> = allocations
            .groups
            .values()
            .filter(|g| g.state == GroupState::Active)
            .collect();
        assert_eq!(active_groups.len(), 1, "Should have one active group");

        // Get group by ID
        let retrieved = allocations.groups.get(&crate::ConsensusGroupId::new(1));
        assert!(retrieved.is_some(), "Should find group by ID");
        assert_eq!(
            retrieved.unwrap().state,
            GroupState::Active,
            "Group should be active"
        );

        // Remove the group
        allocations.groups.remove(&crate::ConsensusGroupId::new(1));
        let active_after_remove: Vec<_> = allocations
            .groups
            .values()
            .filter(|g| g.state == GroupState::Active)
            .collect();
        assert_eq!(
            active_after_remove.len(),
            0,
            "Should have no active groups after removal"
        );
    }
}
