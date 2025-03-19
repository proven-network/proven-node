//! Manages network configuration and point-to-point node communication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
pub use error::Error;

use ed25519_dalek::{SigningKey, VerifyingKey};
use hex;
use proven_attestation::Attestor;
use proven_governance::{Governance, TopologyNode};

/// Options for creating a ProvenNetwork instance.
#[derive(Debug, Clone)]
pub struct ProvenNetworkOptions<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// The attestor implementation to use.
    pub attestor: A,

    /// The governance implementation to use.
    pub governance: G,

    /// The private key in hex format.
    pub private_key_hex: String,
}

/// Proven Network implementation.
#[derive(Clone)]
pub struct ProvenNetwork<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// The attestor implementation to use.
    #[allow(dead_code)]
    attestor: A,

    /// The governance implementation to use.
    governance: G,

    /// The private key of this node in hex format.
    private_key: SigningKey,

    /// The public key of the node.
    ///
    /// This is the public key of the node.
    public_key: VerifyingKey,
}

impl<G, A> ProvenNetwork<G, A>
where
    G: Governance,
    A: Attestor,
{
    /// Create a new ProvenNetwork instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The private key is invalid
    pub fn new(
        ProvenNetworkOptions {
            attestor,
            governance,
            private_key_hex,
        }: ProvenNetworkOptions<G, A>,
    ) -> Result<Self, Error> {
        // Parse the private key and calculate public key
        let private_key_bytes = hex::decode(private_key_hex.trim()).map_err(|e| {
            Error::PrivateKey(format!("Failed to decode private key as hex: {}", e))
        })?;

        // We need exactly 32 bytes for ed25519 private key
        let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
            Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
        })?;

        // Derive the public key
        let public_key = private_key.verifying_key();

        Ok(Self {
            attestor,
            governance,
            private_key,
            public_key,
        })
    }

    /// Get all peer nodes in the network topology (excluding self).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get topology from governance
    /// - Failed to get self node
    pub async fn get_peers(&self) -> Result<Vec<TopologyNode>, Error> {
        let self_node = self.get_self().await?;
        let all_nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(e.to_string()))?;

        // Filter out the self node
        Ok(all_nodes
            .into_iter()
            .filter(|node| node.public_key != self_node.public_key)
            .collect())
    }

    /// Get the node definition for this node based on the private key.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get topology from governance
    /// - Self node not found in topology
    pub async fn get_self(&self) -> Result<TopologyNode, Error> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(e.to_string()))?;

        // Find the node with matching public_key in the topology
        let node = topology
            .iter()
            .find(|n| n.public_key == hex::encode(self.public_key.as_bytes()))
            .ok_or_else(|| {
                Error::NodeNotFound(format!(
                    "Node with public key {} not found in topology",
                    hex::encode(self.public_key.as_bytes())
                ))
            })?;

        Ok(node.clone())
    }

    /// Get the governance implementation used by this network.
    #[must_use]
    pub fn governance(&self) -> &G {
        &self.governance
    }

    /// Get the private key used by this network.
    #[must_use]
    pub fn private_key(&self) -> &SigningKey {
        &self.private_key
    }

    /// Get the public key used by this network.
    #[must_use]
    pub fn public_key(&self) -> &VerifyingKey {
        &self.public_key
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proven_attestation_mock::MockAttestor;
    use proven_governance::TopologyNode;
    use proven_governance_mock::MockGovernance;

    use super::*;

    #[tokio::test]
    async fn test_network() {
        // Private key (use a test key that's fixed)
        let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
        // Public key is derived from private key (known derivation for the test key)
        let public_key = "4cb5abf6ad79fbf5abbccafcc269d85cd2651ed4b885b5869f241aedf0a5ba29";

        // Create nodes
        let node1 = TopologyNode {
            availability_zone: "az1".to_string(),
            fqdn: "node1.example.com".to_string(),
            public_key: public_key.to_string(),
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

        // Create mock governance
        let mock_governance = MockGovernance::new(vec![node1.clone(), node2.clone()], vec![]);

        // Create network
        let network = ProvenNetwork::new(ProvenNetworkOptions {
            attestor: MockAttestor,
            governance: mock_governance,
            private_key_hex: private_key.to_string(),
        })
        .unwrap();

        // Test get_self
        let self_node = network.get_self().await.unwrap();
        assert_eq!(self_node.public_key, public_key);

        // Test get_peers
        let peers = network.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].public_key, "other_key");
    }
}
