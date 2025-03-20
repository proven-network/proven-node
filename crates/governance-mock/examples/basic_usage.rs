use std::collections::HashSet;
use std::time::SystemTime;

use proven_governance::{Governance, NodeSpecialization, TopologyNode, Version};
use proven_governance_mock::MockGovernance;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create test nodes
    let node1 = TopologyNode {
        availability_zone: "az1".to_string(),
        origin: "http://node1.example.com".to_string(),
        public_key: "key1".to_string(),
        region: "region1".to_string(),
        specializations: HashSet::new(),
    };

    let node2 = TopologyNode {
        availability_zone: "az2".to_string(),
        origin: "http://node2.example.com".to_string(),
        public_key: "key2".to_string(),
        region: "region2".to_string(),
        specializations: {
            let mut specs = HashSet::new();
            specs.insert(NodeSpecialization::RadixMainnet);
            specs
        },
    };

    // Create test versions
    let version1 = Version {
        activated_at: SystemTime::now(),
        ne_pcr0: "pcr0-1".to_string(),
        ne_pcr1: "pcr1-1".to_string(),
        ne_pcr2: "pcr2-1".to_string(),
        sequence: 1,
    };

    let version2 = Version {
        activated_at: SystemTime::now(),
        ne_pcr0: "pcr0-2".to_string(),
        ne_pcr1: "pcr1-2".to_string(),
        ne_pcr2: "pcr2-2".to_string(),
        sequence: 2,
    };

    // Create mock governance
    let nodes = vec![node1, node2];
    let versions = vec![version1, version2];
    let governance = MockGovernance::new(nodes, versions);

    // Get topology
    let topology = governance.get_topology().await?;
    println!("Topology:");
    for node in topology {
        println!("  - Node ID: {}, Origin: {}", node.public_key, node.origin);
    }

    // Get active versions
    let active_versions = governance.get_active_versions().await?;
    println!("\nActive versions:");
    for version in active_versions {
        println!("  - Version sequence: {}", version.sequence);
    }

    Ok(())
}
