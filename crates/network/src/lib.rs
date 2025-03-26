//! Manages network configuration and point-to-point node communication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod peer;

use std::collections::HashSet;
use std::sync::Arc;

pub use error::Error;
pub use peer::Peer;

use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey, ed25519::signature::SignerMut};
use hex;
use proven_attestation::{AttestationParams, Attestor};
use proven_governance::{Governance, NodeSpecialization};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use url::Url;

/// A signed data structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedData {
    /// The data.
    data: Bytes,

    /// The signature of the data.
    signature: Bytes,
}

impl TryFrom<Bytes> for SignedData {
    type Error = Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
            .map_err(|e| Error::PrivateKey(format!("Failed to deserialize signed data: {}", e)))
    }
}

impl TryInto<Bytes> for SignedData {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&self, &mut payload)
            .map_err(|e| Error::PrivateKey(format!("Failed to serialize signed data: {}", e)))?;
        Ok(Bytes::from(payload))
    }
}

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

    /// The NATS cluster port on the local machine.
    pub nats_cluster_port: u16,

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

    /// The NATS cluster port of this node.
    nats_cluster_port: u16,

    /// The private key of this node.
    private_key: Arc<Mutex<SigningKey>>,

    /// The public key of this node.
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
            nats_cluster_port,
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
            nats_cluster_port,
            private_key: Arc::new(Mutex::new(private_key)),
            public_key,
        })
    }

    /// Attest the nats cluster endpoint.
    pub async fn attested_nats_cluster_endpoint(&self, nonce: Bytes) -> Result<Bytes, Error> {
        let nats_cluster_endpoint = self.nats_cluster_endpoint().await?;
        let signed_data = self.sign_data(nats_cluster_endpoint.as_bytes()).await?;

        let mut payload = Vec::new();
        ciborium::ser::into_writer(&signed_data, &mut payload)
            .map_err(|_| Error::PrivateKey("Failed to serialize signed data".to_string()))?;

        let payload = Bytes::from(payload);

        let attestation = self
            .attestor
            .attest(AttestationParams {
                nonce: Some(&nonce),
                public_key: Some(&self.public_key_bytes()),
                user_data: Some(&payload),
            })
            .await
            .map_err(|e| Error::Attestation(e.to_string()))?;

        Ok(attestation)
    }

    /// Get the availability zone of this node.
    #[must_use]
    pub async fn availability_zone(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.availability_zone().to_string())
    }

    /// Get all peer nodes in the network topology (excluding self).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get topology from governance
    /// - Failed to get self node
    pub async fn get_peers(&self) -> Result<Vec<Peer>, Error> {
        let self_node = self.get_self().await?;
        let all_nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(e.to_string()))?;

        // Filter out the self node
        Ok(all_nodes
            .into_iter()
            .filter(|node| node.public_key != self_node.public_key())
            .map(Peer::from)
            .collect())
    }

    /// FQDN of the node.
    pub async fn fqdn(&self) -> Result<String, Error> {
        let url = Url::parse(self.origin().await?.as_str())?;

        Ok(url.host_str().ok_or(Error::BadOrigin)?.to_string())
    }

    /// Get the governance implementation used by this network.
    #[must_use]
    pub fn governance(&self) -> &G {
        &self.governance
    }

    /// Get the nats cluster endpoint of this node.
    #[must_use]
    pub async fn nats_cluster_endpoint(&self) -> Result<String, Error> {
        let mut url = Url::parse(self.origin().await?.as_str())?;

        url.set_port(Some(self.nats_cluster_port))
            .map_err(|_| Error::BadOrigin)?;
        url.set_scheme("nats").map_err(|_| Error::BadOrigin)?;

        Ok(url.to_string())
    }

    /// Get the origin of this node.
    #[must_use]
    pub async fn origin(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.origin().to_string())
    }

    /// Get the private key used by this node.
    #[must_use]
    pub async fn private_key(&self) -> tokio::sync::MutexGuard<'_, SigningKey> {
        self.private_key.lock().await
    }

    /// Get the public key used by this node.
    #[must_use]
    pub fn public_key(&self) -> &VerifyingKey {
        &self.public_key
    }

    /// Get the public key used by this node as bytes.
    #[must_use]
    pub fn public_key_bytes(&self) -> Bytes {
        Bytes::from(self.public_key.as_bytes().to_vec())
    }

    /// Get the region of this node.
    #[must_use]
    pub async fn region(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.region().to_string())
    }

    /// Get the specializations of this node.
    #[must_use]
    pub async fn specializations(&self) -> Result<HashSet<NodeSpecialization>, Error> {
        Ok(self.get_self().await?.specializations().clone())
    }

    /// Get the self node.
    async fn get_self(&self) -> Result<Peer, Error> {
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

        Ok(node.clone().into())
    }

    /// Sign data with the private key.
    async fn sign_data(&self, data: &[u8]) -> Result<SignedData, Error> {
        let signature = self
            .private_key
            .lock()
            .await
            .try_sign(data)
            .map_err(|e| Error::PrivateKey(e.to_string()))?;

        Ok(SignedData {
            data: Bytes::from(data.to_vec()),
            signature: Bytes::from(signature.to_vec()),
        })
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
            origin: "http://node1.example.com".to_string(),
            public_key: public_key.to_string(),
            region: "region1".to_string(),
            specializations: HashSet::new(),
        };

        let node2 = TopologyNode {
            availability_zone: "az2".to_string(),
            origin: "http://node2.example.com".to_string(),
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
            nats_cluster_port: 6222,
            private_key_hex: private_key.to_string(),
        })
        .unwrap();

        // Test get_self
        let self_node = network.get_self().await.unwrap();
        assert_eq!(self_node.public_key(), public_key);

        // Test get_peers
        let peers = network.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].public_key(), "other_key");
    }
}
