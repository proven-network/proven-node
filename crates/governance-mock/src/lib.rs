//! Mock implementation of the governance interface for testing purposes.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::vec::Vec;

use async_trait::async_trait;
use proven_governance::{Governance, NodeSpecialization, TopologyNode, Version};
use serde::{Deserialize, Serialize};

mod error;
pub use error::Error;

/// Node definition in the topology file
#[derive(Debug, Serialize, Deserialize)]
struct TopologyFileNode {
    origin: String,
    public_key: String,
    specializations: Vec<String>,
}

/// Mock implementation of the governance interface.
#[derive(Debug, Clone)]
pub struct MockGovernance {
    nodes: Vec<TopologyNode>,
    versions: Vec<Version>,
}

impl MockGovernance {
    /// Create a new mock governance implementation with the given nodes and versions.
    #[must_use]
    pub fn new(nodes: Vec<TopologyNode>, versions: Vec<Version>) -> Self {
        Self { nodes, versions }
    }

    /// Create a new mock governance instance from a topology file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The topology file cannot be read
    /// - The topology file contains invalid JSON
    pub fn from_topology_file<P: AsRef<Path>>(
        topology_path: P,
        versions: Vec<Version>,
    ) -> Result<Self, Error> {
        // Read the topology file
        let mut file = File::open(topology_path)
            .map_err(|e| Error::TopologyFile(format!("Failed to open topology file: {}", e)))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|e| Error::TopologyFile(format!("Failed to read topology file: {}", e)))?;

        // Parse the topology nodes
        let topology_nodes: Vec<TopologyFileNode> = serde_json::from_str(&content)
            .map_err(|e| Error::TopologyFile(format!("Failed to parse topology file: {}", e)))?;

        // Convert to governance nodes
        let nodes = topology_nodes
            .into_iter()
            .map(|n| {
                let mut specializations = HashSet::new();
                for spec in n.specializations {
                    if spec == "bitcoin-mainnet" {
                        specializations.insert(NodeSpecialization::BitcoinMainnet);
                    } else if spec == "bitcoin-testnet" {
                        specializations.insert(NodeSpecialization::BitcoinTestnet);
                    } else if spec == "radix-mainnet" {
                        specializations.insert(NodeSpecialization::RadixMainnet);
                    } else if spec == "radix-stokenet" {
                        specializations.insert(NodeSpecialization::RadixStokenet);
                    } else if spec == "ethereum-holesky" {
                        specializations.insert(NodeSpecialization::EthereumHolesky);
                    } else if spec == "ethereum-mainnet" {
                        specializations.insert(NodeSpecialization::EthereumMainnet);
                    } else if spec == "ethereum-sepolia" {
                        specializations.insert(NodeSpecialization::EthereumSepolia);
                    }
                }

                TopologyNode {
                    availability_zone: "local".to_string(),
                    origin: n.origin.clone(),
                    public_key: n.public_key,
                    region: "local".to_string(),
                    specializations,
                }
            })
            .collect();

        Ok(Self { nodes, versions })
    }
}

#[async_trait]
impl Governance for MockGovernance {
    type Error = Error;

    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error> {
        Ok(self.versions.clone())
    }

    async fn get_topology(&self) -> Result<Vec<TopologyNode>, Self::Error> {
        Ok(self.nodes.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::SystemTime;

    use proven_governance::{NodeSpecialization, TopologyNode, Version};

    use super::*;

    #[tokio::test]
    async fn test_mock_governance() {
        // Create test nodes
        let node_1 = TopologyNode {
            availability_zone: "az1".to_string(),
            origin: "http://node1.example.com".to_string(),
            public_key: "key1".to_string(),
            region: "region1".to_string(),
            specializations: HashSet::new(),
        };

        let node_2 = TopologyNode {
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
        let version_1 = Version {
            activated_at: SystemTime::now(),
            ne_pcr0: "pcr0-1".to_string(),
            ne_pcr1: "pcr1-1".to_string(),
            ne_pcr2: "pcr2-1".to_string(),
            sequence: 1,
        };

        let version_2 = Version {
            activated_at: SystemTime::now(),
            ne_pcr0: "pcr0-2".to_string(),
            ne_pcr1: "pcr1-2".to_string(),
            ne_pcr2: "pcr2-2".to_string(),
            sequence: 2,
        };

        // Create mock governance
        let governance = MockGovernance::new(
            vec![node_1.clone(), node_2.clone()],
            vec![version_1.clone(), version_2.clone()],
        );

        // Test get_topology
        let topology = governance.get_topology().await.unwrap();
        assert_eq!(topology.len(), 2);
        assert!(topology.contains(&node_1));
        assert!(topology.contains(&node_2));

        // Test get_active_versions
        let active_versions = governance.get_active_versions().await.unwrap();
        assert_eq!(active_versions.len(), 2);
        assert!(active_versions.contains(&version_1));
        assert!(active_versions.contains(&version_2));
    }
}
