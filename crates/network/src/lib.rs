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
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{SigningKey, VerifyingKey};
use proven_attestation::{AttestationParams, Attestor};
use proven_governance::{Governance, NodeSpecialization, Version};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use url::Url;

/// The API path for the nats cluster endpoint.
pub static NATS_CLUSTER_ENDPOINT_API_PATH: &str = "/v1/node/nats-cluster-endpoint";

/// An attested and signed data structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestedData {
    /// The attestation of the data (signature is included in the attestation)
    pub attestation: Bytes,

    /// The data.
    pub data: Bytes,
}

/// A signed data structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedData {
    /// The data.
    pub data: Bytes,

    /// The signature of the data.
    pub signature: Bytes,
}

/// Options for creating a `ProvenNetwork` instance.
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

    /// The private key of the current node.
    pub private_key: SigningKey,
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

    /// The username for the NATS cluster.
    nats_cluster_username: String,

    /// The password for the NATS cluster.
    nats_cluster_password: String,

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
    /// Create a new `ProvenNetwork` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The private key is invalid
    pub async fn new(
        ProvenNetworkOptions {
            attestor,
            governance,
            nats_cluster_port,
            private_key,
        }: ProvenNetworkOptions<G, A>,
    ) -> Result<Self, Error> {
        // Derive the public key
        let public_key = private_key.verifying_key();

        let random_bytes = attestor
            .secure_random()
            .await
            .map_err(|e| Error::Attestation(format!("Failed to generate random bytes: {e}")))?;
        let nats_cluster_password = hex::encode(random_bytes);

        Ok(Self {
            attestor,
            governance,
            nats_cluster_port,
            nats_cluster_username: hex::encode(public_key.as_bytes()),
            nats_cluster_password,
            private_key: Arc::new(Mutex::new(private_key)),
            public_key,
        })
    }

    /// Get the active versions of the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get active versions from governance
    pub async fn get_active_versions(&self) -> Result<Vec<Version>, Error> {
        self.governance
            .get_active_versions()
            .await
            .map_err(|e| Error::Governance(e.to_string()))
    }

    /// Attest the nats cluster endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get nats cluster endpoint
    /// - Failed to attest data
    pub async fn attested_nats_cluster_endpoint(
        &self,
        nonce: Bytes,
    ) -> Result<AttestedData, Error> {
        let nats_cluster_endpoint = self.nats_cluster_endpoint().await?;
        let nats_cluster_endpoint_bytes = nats_cluster_endpoint.to_string().as_bytes().to_vec();

        self.attest_data(&nonce, &nats_cluster_endpoint_bytes).await
    }

    /// Get the availability zone of this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get self node
    pub async fn availability_zone(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.availability_zone().to_string())
    }

    /// Get the attestor instance used by this network.
    #[must_use]
    pub const fn attestor(&self) -> &A {
        &self.attestor
    }

    /// Get all peer nodes in the network topology (excluding self).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get topology from governance
    /// - Failed to get self node
    /// - Failed to get active versions
    pub async fn get_peers(&self) -> Result<Vec<Peer<A>>, Error> {
        let all_nodes = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(e.to_string()))?;

        let active_versions = self.get_active_versions().await?;

        // Filter out self
        Ok(all_nodes
            .into_iter()
            .filter(|node| node.public_key != self.public_key)
            .map(|node| Peer::new(node, active_versions.clone(), self.attestor.clone()))
            .collect())
    }

    /// FQDN of the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to parse origin
    /// - Failed to get host string from URL
    pub async fn fqdn(&self) -> Result<String, Error> {
        let url = Url::parse(self.origin().await?.as_str())?;

        Ok(url.host_str().ok_or(Error::BadOrigin)?.to_string())
    }

    /// Get the governance implementation used by this network.
    #[must_use]
    pub const fn governance(&self) -> &G {
        &self.governance
    }

    /// Get the nats cluster endpoint of this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to parse origin
    /// - Failed to set username
    /// - Failed to set password
    pub async fn nats_cluster_endpoint(&self) -> Result<Url, Error> {
        let fqdn = self.fqdn().await?;
        let mut url = Url::parse(&format!("nats://{}:{}", fqdn, self.nats_cluster_port))?;

        url.set_username(&self.nats_cluster_username)
            .map_err(|()| Error::GenerateClusterEndpoint("failed to set username"))?;
        url.set_password(Some(&self.nats_cluster_password))
            .map_err(|()| Error::GenerateClusterEndpoint("failed to set password"))?;

        Ok(url)
    }

    /// Get the origin of this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get self node
    pub async fn origin(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.origin().to_string())
    }

    /// Get the private key used by this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get private key
    pub async fn private_key(&self) -> tokio::sync::MutexGuard<'_, SigningKey> {
        self.private_key.lock().await
    }

    /// Get the public key used by this node.
    #[must_use]
    pub const fn public_key(&self) -> &VerifyingKey {
        &self.public_key
    }

    /// Get the public key used by this node as bytes.
    #[must_use]
    pub fn public_key_bytes(&self) -> Bytes {
        Bytes::from(self.public_key.as_bytes().to_vec())
    }

    /// Get the region of this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get self node
    pub async fn region(&self) -> Result<String, Error> {
        Ok(self.get_self().await?.region().to_string())
    }

    /// Get the specializations of this node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get self node
    pub async fn specializations(&self) -> Result<HashSet<NodeSpecialization>, Error> {
        Ok(self.get_self().await?.specializations().clone())
    }

    /// Get the self node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to get topology from governance
    /// - Failed to find self node in topology
    /// - Failed to get active versions
    async fn get_self(&self) -> Result<Peer<A>, Error> {
        let topology = self
            .governance
            .get_topology()
            .await
            .map_err(|e| Error::Governance(e.to_string()))?;

        let node = topology
            .iter()
            .find(|n| n.public_key == self.public_key)
            .ok_or_else(|| {
                Error::NodeNotFound(format!(
                    "Node with public key {} not found in topology",
                    hex::encode(self.public_key.as_bytes())
                ))
            })?;

        // Use Peer::new with cloned attestor
        Ok(Peer::new(
            node.clone(),
            self.get_active_versions().await?,
            self.attestor.clone(),
        ))
    }

    /// Attest the provided data.
    async fn attest_data(
        &self,
        nonce: &Bytes,
        data_to_attest: &[u8],
    ) -> Result<AttestedData, Error> {
        let signed_data = self.sign_data(data_to_attest).await?;

        let attestation = self
            .attestor
            .attest(AttestationParams {
                nonce: Some(nonce.clone()),
                public_key: Some(self.public_key_bytes()),
                user_data: Some(signed_data.signature.clone()),
            })
            .await
            .map_err(|e| Error::Attestation(e.to_string()))?;

        Ok(AttestedData {
            attestation,
            data: Bytes::from(data_to_attest.to_vec()),
        })
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
    use proven_governance::GovernanceNode;
    use proven_governance_mock::MockGovernance;

    use super::*;

    #[tokio::test]
    async fn test_network() {
        // Private key (use a test key that's fixed)
        let private_key_hex = "0000000000000000000000000000000000000000000000000000000000000001";
        // Public key is derived from private key (known derivation for the test key)
        let public_key_hex = "4cb5abf6ad79fbf5abbccafcc269d85cd2651ed4b885b5869f241aedf0a5ba29";

        let public_key =
            VerifyingKey::from_bytes(&hex::decode(public_key_hex).unwrap().try_into().unwrap())
                .unwrap();

        // Create nodes
        let node1 = GovernanceNode {
            availability_zone: "az1".to_string(),
            origin: "http://node1.example.com".to_string(),
            public_key,
            region: "region1".to_string(),
            specializations: HashSet::new(),
        };

        let node2 = GovernanceNode {
            availability_zone: "az2".to_string(),
            origin: "http://node2.example.com".to_string(),
            public_key: VerifyingKey::from_bytes(
                &hex::decode("other_key").unwrap().try_into().unwrap(),
            )
            .unwrap(),
            region: "region2".to_string(),
            specializations: HashSet::new(),
        };

        // Create mock governance
        let mock_governance = MockGovernance::new(
            vec![node1.clone(), node2.clone()],
            vec![],
            "http://localhost:3200".to_string(),
            vec![],
        );

        // Create network
        let network = ProvenNetwork::new(ProvenNetworkOptions {
            attestor: MockAttestor::new(),
            governance: mock_governance,
            nats_cluster_port: 6222,
            private_key: SigningKey::from_bytes(
                &hex::decode(private_key_hex)
                    .unwrap()
                    .try_into()
                    .expect("Private key hex must be 32 bytes"),
            ),
        })
        .await
        .unwrap();

        // Test get_self
        let self_node = network.get_self().await.unwrap();
        assert_eq!(self_node.public_key(), network.public_key());

        // Test get_peers
        let peers = network.get_peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].public_key(), network.public_key());
    }
}
