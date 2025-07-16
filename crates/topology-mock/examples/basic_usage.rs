use std::collections::HashSet;

use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use proven_topology::{Node, NodeId, NodeSpecialization, TopologyAdaptor, Version};
use proven_topology_mock::MockTopologyAdaptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create test nodes
    let node1 = Node::new(
        "az1".to_string(),
        "http://node1.example.com".to_string(),
        NodeId::from(VerifyingKey::from_bytes(&[0; 32]).unwrap()),
        "region1".to_string(),
        HashSet::new(),
    );

    let node2 = Node::new(
        "az2".to_string(),
        "http://node2.example.com".to_string(),
        NodeId::from(VerifyingKey::from_bytes(&[1; 32]).unwrap()),
        "region2".to_string(),
        {
            let mut specs = HashSet::new();
            specs.insert(NodeSpecialization::RadixMainnet);
            specs
        },
    );

    // Create test versions
    let version1 = Version {
        ne_pcr0: Bytes::from("pcr0-1"),
        ne_pcr1: Bytes::from("pcr1-1"),
        ne_pcr2: Bytes::from("pcr2-1"),
    };

    let version2 = Version {
        ne_pcr0: Bytes::from("pcr0-2"),
        ne_pcr1: Bytes::from("pcr1-2"),
        ne_pcr2: Bytes::from("pcr2-2"),
    };

    // Create mock governance
    let nodes = vec![node1, node2];
    let versions = vec![version1, version2];
    let governance =
        MockTopologyAdaptor::new(nodes, versions, "http://localhost:3200".to_string(), vec![]);

    // Get topology
    let topology = governance.get_topology().await?;
    println!("Topology:");
    for node in topology {
        println!("  - Node ID: {}, Origin: {}", node.node_id, node.origin);
    }

    // Get active versions
    let active_versions = governance.get_active_versions().await?;
    println!("\nActive versions:");
    for version in active_versions {
        println!(
            "  - Version: PCR0: {}, PCR1: {}, PCR2: {}",
            hex::encode(version.ne_pcr0),
            hex::encode(version.ne_pcr1),
            hex::encode(version.ne_pcr2)
        );
    }

    Ok(())
}
