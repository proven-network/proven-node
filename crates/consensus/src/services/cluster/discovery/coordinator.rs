//! Coordinator election logic for cluster discovery

use proven_topology::NodeId;
use std::collections::HashSet;
use tracing::{debug, info};

/// Coordinator election logic
pub struct CoordinatorElection;

impl CoordinatorElection {
    /// Determine if this node should become the coordinator
    ///
    /// The coordinator is elected deterministically based on node IDs.
    /// The node with the lexicographically smallest ID becomes coordinator.
    pub fn should_become_coordinator(local_node_id: &NodeId, discovered_peers: &[NodeId]) -> bool {
        // Create a set of all nodes including ourselves
        let mut all_nodes = HashSet::new();
        all_nodes.insert(local_node_id.clone());
        for peer in discovered_peers {
            all_nodes.insert(peer.clone());
        }

        // Sort nodes deterministically
        let mut sorted_nodes: Vec<_> = all_nodes.into_iter().collect();
        sorted_nodes.sort();

        debug!(
            "Coordinator election: {} nodes participating, local node: {}",
            sorted_nodes.len(),
            local_node_id
        );

        // The first node in sorted order becomes coordinator
        if let Some(coordinator) = sorted_nodes.first() {
            let is_coordinator = coordinator == local_node_id;
            info!(
                "Coordinator election result: {} (local: {}, coordinator: {})",
                if is_coordinator {
                    "ELECTED"
                } else {
                    "NOT ELECTED"
                },
                local_node_id,
                coordinator
            );
            is_coordinator
        } else {
            // Should never happen, but fail-safe to true
            true
        }
    }

    /// Get the expected coordinator from a set of nodes
    pub fn get_expected_coordinator(nodes: &[NodeId]) -> Option<NodeId> {
        if nodes.is_empty() {
            return None;
        }

        let mut sorted_nodes = nodes.to_vec();
        sorted_nodes.sort();
        sorted_nodes.first().cloned()
    }

    /// Calculate delay before attempting coordinator election
    ///
    /// Non-coordinator nodes should wait slightly longer to allow
    /// the coordinator to initialize first.
    pub fn calculate_election_delay(
        local_node_id: &NodeId,
        discovered_peers: &[NodeId],
    ) -> std::time::Duration {
        if Self::should_become_coordinator(local_node_id, discovered_peers) {
            // Coordinator proceeds immediately
            std::time::Duration::from_millis(0)
        } else {
            // Non-coordinators wait a bit
            std::time::Duration::from_secs(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinator_election_deterministic() {
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);
        let node3 = NodeId::from_seed(3);

        // Figure out which node should be coordinator by sorting
        let mut nodes = vec![node1.clone(), node2.clone(), node3.clone()];
        nodes.sort();
        let expected_coordinator = nodes[0].clone();

        // Verify the election returns consistent results
        let is_node1_coordinator =
            CoordinatorElection::should_become_coordinator(&node1, &[node2.clone(), node3.clone()]);
        let is_node2_coordinator =
            CoordinatorElection::should_become_coordinator(&node2, &[node1.clone(), node3.clone()]);
        let is_node3_coordinator =
            CoordinatorElection::should_become_coordinator(&node3, &[node1.clone(), node2.clone()]);

        // Exactly one should be coordinator
        let coordinator_count = [
            is_node1_coordinator,
            is_node2_coordinator,
            is_node3_coordinator,
        ]
        .iter()
        .filter(|&&x| x)
        .count();
        assert_eq!(
            coordinator_count, 1,
            "Exactly one node should be coordinator"
        );

        // The coordinator should match expected
        if expected_coordinator == node1 {
            assert!(is_node1_coordinator);
        } else if expected_coordinator == node2 {
            assert!(is_node2_coordinator);
        } else {
            assert!(is_node3_coordinator);
        }
    }

    #[test]
    fn test_coordinator_election_single_node() {
        let node = NodeId::from_seed(1);
        assert!(CoordinatorElection::should_become_coordinator(&node, &[]));
    }

    #[test]
    fn test_get_expected_coordinator() {
        let node1 = NodeId::from_seed(3);
        let node2 = NodeId::from_seed(1);
        let node3 = NodeId::from_seed(2);

        let nodes = vec![node1.clone(), node2.clone(), node3.clone()];
        let coordinator = CoordinatorElection::get_expected_coordinator(&nodes);

        // Should return a coordinator
        assert!(coordinator.is_some());

        // The coordinator should be one of the nodes
        assert!(nodes.contains(coordinator.as_ref().unwrap()));

        // It should be deterministic - calling again returns the same result
        let coordinator2 = CoordinatorElection::get_expected_coordinator(&nodes);
        assert_eq!(coordinator, coordinator2);

        // The coordinator should be the lexicographically smallest
        let mut sorted = nodes.clone();
        sorted.sort();
        assert_eq!(coordinator, Some(sorted[0].clone()));
    }
}
