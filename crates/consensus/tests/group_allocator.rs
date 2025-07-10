mod common;

use common::create_test_topology_manager;

use proven_consensus::NodeId;
use proven_consensus::allocation::ConsensusGroupId;
use proven_consensus::group_allocator::ConsensusGroup;
use proven_consensus::group_allocator::GroupAllocations;
use proven_consensus::group_allocator::GroupAllocatorConfig;
use proven_consensus::group_allocator::GroupRebalancer;
use proven_consensus::group_allocator::GroupState;
use proven_consensus::group_allocator::TopologyAnalyzer;

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
        id: ConsensusGroupId::new(1),
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
