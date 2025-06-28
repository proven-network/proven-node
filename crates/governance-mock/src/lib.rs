//! Mock implementation of the governance interface for testing purposes.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod config;
mod error;

use config::Config;
pub use error::Error;

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use proven_governance::{Governance, NodeSpecialization, TopologyNode, Version};

/// Mock implementation of the governance interface.
#[derive(Debug, Clone)]
pub struct MockGovernance {
    alternate_auth_gateways: Vec<String>,
    primary_auth_gateway: String,
    nodes: Arc<Mutex<Vec<TopologyNode>>>,
    versions: Vec<Version>,
}

impl MockGovernance {
    /// Create a new mock governance implementation with the given nodes and versions.
    #[must_use]
    pub fn new(
        nodes: Vec<TopologyNode>,
        versions: Vec<Version>,
        primary_auth_gateway: String,
        alternate_auth_gateways: Vec<String>,
    ) -> Self {
        Self {
            alternate_auth_gateways,
            primary_auth_gateway,
            nodes: Arc::new(Mutex::new(nodes)),
            versions,
        }
    }

    /// Create a new mock governance implementation with a single node.
    #[must_use]
    pub fn for_single_node(origin: String, private_key: &SigningKey, version: Version) -> Self {
        let node = TopologyNode {
            availability_zone: "local".to_string(),
            origin,
            public_key: hex::encode(private_key.verifying_key().to_bytes()),
            region: "local".to_string(),
            specializations: HashSet::new(),
        };

        Self::new(
            vec![node],
            vec![version],
            "http://localhost:3200".to_string(),
            vec![],
        )
    }

    /// Create a new mock governance instance from a topology file
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The network config file cannot be read
    /// - The network config file contains invalid JSON
    ///
    /// # Panics
    ///
    /// Panics if the network config file cannot be read.
    pub fn from_network_config_file<P: AsRef<Path>>(network_config_path: P) -> Result<Self, Error> {
        // Read the network config file
        let mut file = File::open(network_config_path)
            .map_err(|e| Error::TopologyFile(format!("Failed to open network config file: {e}")))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .map_err(|e| Error::TopologyFile(format!("Failed to read network config file: {e}")))?;

        // Parse the network config file
        let network_config: Config = serde_json::from_str(&content).map_err(|e| {
            Error::TopologyFile(format!("Failed to parse network config file: {e}"))
        })?;

        // Convert to governance nodes
        let nodes = network_config
            .topology
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

        let versions = network_config
            .versions
            .into_iter()
            .map(|v| Version {
                ne_pcr0: Bytes::from(hex::decode(v.pcr0).unwrap()),
                ne_pcr1: Bytes::from(hex::decode(v.pcr1).unwrap()),
                ne_pcr2: Bytes::from(hex::decode(v.pcr2).unwrap()),
            })
            .collect();

        Ok(Self {
            alternate_auth_gateways: network_config.auth_gateways.alternates,
            primary_auth_gateway: network_config.auth_gateways.primary,
            nodes: Arc::new(Mutex::new(nodes)),
            versions,
        })
    }

    /// Add a node to the topology.
    ///
    /// # Errors
    ///
    /// Returns an error if a node with the same public key already exists.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    pub fn add_node(&self, node: TopologyNode) -> Result<(), Error> {
        let mut nodes = self.nodes.lock().unwrap();

        // Check if node already exists
        if nodes.iter().any(|n| n.public_key == node.public_key) {
            return Err(Error::NodeManagement(format!(
                "Node with public key {} already exists",
                node.public_key
            )));
        }

        nodes.push(node);
        drop(nodes);
        Ok(())
    }

    /// Remove a node from the topology by public key.
    ///
    /// # Errors
    ///
    /// Returns an error if no node with the given public key is found.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    pub fn remove_node(&self, public_key: &str) -> Result<(), Error> {
        let mut nodes = self.nodes.lock().unwrap();

        let original_len = nodes.len();
        nodes.retain(|n| n.public_key != public_key);

        if nodes.len() == original_len {
            return Err(Error::NodeNotFound(format!(
                "Node with public key {public_key} not found"
            )));
        }
        drop(nodes);

        Ok(())
    }

    /// Check if a node exists by public key.
    ///
    /// # Panics
    ///
    /// Panics if the nodes cannot be locked.
    #[must_use]
    pub fn has_node(&self, public_key: &str) -> bool {
        let nodes = self.nodes.lock().unwrap();
        nodes.iter().any(|n| n.public_key == public_key)
    }
}

#[async_trait]
impl Governance for MockGovernance {
    type Error = Error;

    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error> {
        Ok(self.versions.clone())
    }

    async fn get_alternates_auth_gateways(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.alternate_auth_gateways.clone())
    }

    async fn get_primary_auth_gateway(&self) -> Result<String, Self::Error> {
        Ok(self.primary_auth_gateway.clone())
    }

    async fn get_topology(&self) -> Result<Vec<TopologyNode>, Self::Error> {
        Ok(self.nodes.lock().unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
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
            ne_pcr0: Bytes::from("pcr0-1"),
            ne_pcr1: Bytes::from("pcr1-1"),
            ne_pcr2: Bytes::from("pcr2-1"),
        };

        let version_2 = Version {
            ne_pcr0: Bytes::from("pcr0-2"),
            ne_pcr1: Bytes::from("pcr1-2"),
            ne_pcr2: Bytes::from("pcr2-2"),
        };

        // Create mock governance
        let governance = MockGovernance::new(
            vec![node_1.clone(), node_2.clone()],
            vec![version_1.clone(), version_2.clone()],
            "http://localhost:3200".to_string(),
            vec![],
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

        // Test get_alternates_auth_gateways
        let alternates = governance.get_alternates_auth_gateways().await.unwrap();
        assert_eq!(alternates.len(), 0);

        // Test get_primary_auth_gateway
        let primary = governance.get_primary_auth_gateway().await.unwrap();
        assert_eq!(primary, "http://localhost:3200");
    }
}
