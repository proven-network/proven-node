use std::collections::HashSet;
use std::time::SystemTime;

use proven_attestation_mock::MockAttestor;
use proven_governance::{Governance, TopologyNode, Version};
use proven_governance_mock::MockGovernance;
use proven_network::{ProvenNetwork, ProvenNetworkOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up a simple mock governance
    let node1 = TopologyNode {
        availability_zone: "az1".to_string(),
        fqdn: "node1.example.com".to_string(),
        public_key: "4cb5abf6ad79fbf5abbccafcc269d85cd2651ed4b885b5869f241aedf0a5ba29".to_string(),
        region: "region1".to_string(),
        specializations: HashSet::new(),
    };

    let node2 = TopologyNode {
        availability_zone: "az2".to_string(),
        fqdn: "node2.example.com".to_string(),
        public_key: "other_key".to_string(),
        region: "region2".to_string(),
        specializations: HashSet::new(),
    };

    let version = Version {
        activated_at: SystemTime::now(),
        ne_pcr0: "pcr0".to_string(),
        ne_pcr1: "pcr1".to_string(),
        ne_pcr2: "pcr2".to_string(),
        sequence: 1,
    };

    // Create a mock governance
    let governance = MockGovernance::new(vec![node1, node2], vec![version]);

    // Create a network with the mock governance
    let network = ProvenNetwork::new(ProvenNetworkOptions {
        attestor: MockAttestor,
        governance,
        private_key_hex: "0000000000000000000000000000000000000000000000000000000000000001"
            .to_string(),
    })?;

    // Use the network to get self and peers
    let self_node = network.get_self().await?;
    println!("Self node: {}", self_node.public_key());

    let peers = network.get_peers().await?;
    println!("Peers: {:?}", peers);

    // Access governance methods through the network
    let versions = network.governance().get_active_versions().await?;
    println!("Active versions: {:?}", versions);

    println!(
        "Public key: {}",
        hex::encode(network.public_key().as_bytes())
    );

    Ok(())
}
