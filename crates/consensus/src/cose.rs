//! COSE (CBOR Object Signing and Encryption) support for secure messaging

use std::collections::BTreeMap;
use std::fmt::Debug;

use bytes::Bytes;
use coset::{
    cbor::value::Value, iana, CoseSign1, CoseSign1Builder, HeaderBuilder, Label,
    TaggedCborSerializable,
};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hex;
use serde::{Deserialize, Serialize};
use tracing::debug;

use proven_governance::Governance;

use crate::error::{ConsensusError, ConsensusResult};

/// COSE-signed message wrapper
#[derive(Debug, Clone)]
pub struct CoseMessage {
    /// The `COSE_Sign1` object containing the signed message
    pub cose_sign1: CoseSign1,
    /// Original payload for convenience (not transmitted)
    pub payload: Option<Bytes>,
}

/// Message payload structure for consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePayload {
    /// The actual consensus message data
    pub data: Bytes,
    /// Sender's node ID
    pub sender_id: String,
    /// Timestamp (unix epoch millis)
    pub timestamp: u64,
    /// Message type identifier
    pub message_type: String,
}

/// COSE message handler for creating and verifying signed messages
pub struct CoseHandler<G>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
{
    /// Node's signing key
    signing_key: SigningKey,
    /// Node ID
    node_id: String,
    /// Governance system for key verification
    governance: G,
}

impl<G> CoseHandler<G>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new COSE handler
    pub const fn new(signing_key: SigningKey, node_id: String, governance: G) -> Self {
        Self {
            signing_key,
            node_id,
            governance,
        }
    }

    /// Create a `COSE_Sign1` message with ed25519 signature
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR serialization fails or COSE signing fails.
    pub fn create_signed_message(
        &self,
        payload: &MessagePayload,
        _additional_headers: Option<BTreeMap<Label, Value>>,
    ) -> ConsensusResult<CoseMessage> {
        // Serialize payload to CBOR
        let mut payload_bytes = Vec::new();
        ciborium::ser::into_writer(payload, &mut payload_bytes).map_err(|e| {
            ConsensusError::InvalidMessage(format!("CBOR serialization failed: {e}"))
        })?;

        // Build protected headers
        let protected_header = HeaderBuilder::new()
            .algorithm(iana::Algorithm::EdDSA)
            .key_id(self.node_id.as_bytes().to_vec())
            .build();

        // Create COSE_Sign1 structure
        let sign1 = CoseSign1Builder::new()
            .protected(protected_header)
            .payload(payload_bytes.clone())
            .build();
        let to_sign = sign1.tbs_data(b"");

        // Sign with ed25519
        let signature = self.signing_key.sign(&to_sign);
        let signature_bytes = signature.to_bytes().to_vec();

        // Build final COSE_Sign1 with signature
        let final_sign1 = CoseSign1Builder::new()
            .protected(sign1.protected.header)
            .payload(payload_bytes.clone())
            .signature(signature_bytes)
            .build();

        debug!(
            "Created COSE_Sign1 message for node {} with {} byte payload",
            self.node_id,
            payload_bytes.len()
        );

        Ok(CoseMessage {
            cose_sign1: final_sign1,
            payload: Some(Bytes::from(payload_bytes)),
        })
    }

    /// Verify a `COSE_Sign1` message and extract the payload
    ///
    /// # Errors
    ///
    /// Returns an error if signature verification fails, CBOR deserialization fails,
    /// or the sender ID doesn't match the expected value.
    pub async fn verify_signed_message(
        &self,
        cose_message: &CoseMessage,
    ) -> ConsensusResult<MessagePayload> {
        let sign1 = &cose_message.cose_sign1;

        // Extract sender's node ID from key_id in protected headers
        let sender_node_id = Self::extract_sender_id(sign1)?;

        // Get sender's public key from governance
        let verifying_key = self.get_public_key_for_node(sender_node_id.clone()).await?;

        // Verify the signature
        let to_verify = sign1.tbs_data(b"");
        let signature_bytes = if sign1.signature.is_empty() {
            return Err(ConsensusError::InvalidMessage(
                "Missing signature".to_string(),
            ));
        } else {
            &sign1.signature
        };

        let signature =
            Signature::from_bytes(signature_bytes.as_slice().try_into().map_err(|_| {
                ConsensusError::InvalidMessage("Invalid signature length".to_string())
            })?);

        verifying_key.verify(&to_verify, &signature).map_err(|e| {
            ConsensusError::InvalidMessage(format!("Signature verification failed: {e}"))
        })?;

        // Extract and deserialize payload
        let payload_bytes = sign1
            .payload
            .as_ref()
            .ok_or_else(|| ConsensusError::InvalidMessage("Missing payload".to_string()))?;

        let payload: MessagePayload =
            ciborium::de::from_reader(payload_bytes.as_slice()).map_err(|e| {
                ConsensusError::InvalidMessage(format!("CBOR deserialization failed: {e}"))
            })?;

        // Verify sender ID matches
        if payload.sender_id != sender_node_id {
            return Err(ConsensusError::InvalidMessage(
                "Sender ID mismatch".to_string(),
            ));
        }

        debug!(
            "Successfully verified COSE_Sign1 message from node {}",
            sender_node_id
        );

        Ok(payload)
    }

    /// Serialize a COSE message to bytes for transmission
    ///
    /// # Errors
    ///
    /// Returns an error if COSE serialization fails.
    pub fn serialize_cose_message(&self, cose_message: &CoseMessage) -> ConsensusResult<Vec<u8>> {
        let cbor_bytes = cose_message
            .cose_sign1
            .clone()
            .to_tagged_vec()
            .map_err(|e| {
                ConsensusError::InvalidMessage(format!("COSE serialization failed: {e}"))
            })?;

        Ok(cbor_bytes)
    }

    /// Deserialize bytes into a COSE message
    ///
    /// # Errors
    ///
    /// Returns an error if COSE deserialization fails.
    pub fn deserialize_cose_message(&self, bytes: &[u8]) -> ConsensusResult<CoseMessage> {
        let cose_sign1 = CoseSign1::from_tagged_slice(bytes).map_err(|e| {
            ConsensusError::InvalidMessage(format!("COSE deserialization failed: {e}"))
        })?;

        Ok(CoseMessage {
            cose_sign1,
            payload: None, // Will be extracted during verification
        })
    }

    /// Extract sender node ID from `COSE_Sign1` protected headers
    fn extract_sender_id(sign1: &CoseSign1) -> ConsensusResult<String> {
        let protected_header = &sign1.protected.header;

        let key_id = &protected_header.key_id;
        if key_id.is_empty() {
            return Err(ConsensusError::InvalidMessage(
                "Missing key_id in header".to_string(),
            ));
        }

        // The key_id contains the hex-encoded public key as UTF-8 bytes
        String::from_utf8(key_id.clone())
            .map_err(|e| ConsensusError::InvalidMessage(format!("Invalid key_id format: {e}")))
    }

    /// Get public key for a node from governance
    async fn get_public_key_for_node(&self, node_id: String) -> ConsensusResult<VerifyingKey> {
        // Get the current topology from governance
        let topology =
            self.governance.get_topology().await.map_err(|e| {
                ConsensusError::InvalidMessage(format!("Failed to get topology: {e}"))
            })?;

        // Find the node in the topology
        // For this implementation, we'll use a simple mapping based on node ID
        // In practice, you'd have a proper node ID to public key mapping

        for node in &topology {
            // Check if this node matches the requested node_id
            // For now, using the public key as the node ID
            if node.public_key == node_id {
                // Parse the hex-encoded public key
                let public_key_bytes = hex::decode(&node.public_key).map_err(|e| {
                    ConsensusError::InvalidMessage(format!("Invalid public key hex: {e}"))
                })?;

                if public_key_bytes.len() != 32 {
                    return Err(ConsensusError::InvalidMessage(
                        "Invalid public key length, expected 32 bytes".to_string(),
                    ));
                }

                let mut key_bytes = [0u8; 32];
                key_bytes.copy_from_slice(&public_key_bytes);

                let verifying_key = VerifyingKey::from_bytes(&key_bytes).map_err(|e| {
                    ConsensusError::InvalidMessage(format!("Invalid public key: {e}"))
                })?;

                debug!("Retrieved public key for node {} from governance", node_id);
                return Ok(verifying_key);
            }
        }

        // If node not found, return error
        Err(ConsensusError::InvalidMessage(format!(
            "Node {node_id} not found in governance topology"
        )))
    }
}

/// Convenience functions for creating message payloads
impl MessagePayload {
    /// Create a new message payload
    ///
    /// # Panics
    ///
    /// Panics if the system time is before the Unix epoch.
    pub fn new(data: Bytes, sender_id: String, message_type: impl Into<String>) -> Self {
        Self {
            data,
            sender_id,
            #[allow(clippy::cast_possible_truncation)]
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message_type: message_type.into(),
        }
    }

    /// Create payload for consensus messages
    pub fn consensus_message(consensus_data: Bytes, sender_id: String) -> Self {
        Self::new(consensus_data, sender_id, "consensus")
    }

    /// Create payload for handshake messages
    pub fn handshake_message(handshake_data: Bytes, sender_id: String) -> Self {
        Self::new(handshake_data, sender_id, "handshake")
    }
}

impl<G> Debug for CoseHandler<G>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CoseHandler")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ed25519_dalek::SigningKey;
    use proven_governance::{TopologyNode, Version};
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_cose_message_creation_and_serialization() {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let public_key = signing_key.verifying_key().to_bytes();
        let node_id = hex::encode(public_key);

        // Create a mock governance with test data
        let test_version = Version {
            ne_pcr0: Bytes::from("test_pcr0"),
            ne_pcr1: Bytes::from("test_pcr1"),
            ne_pcr2: Bytes::from("test_pcr2"),
        };

        let test_node = TopologyNode {
            availability_zone: "test".to_string(),
            origin: "http://test.example.com".to_string(),
            public_key: hex::encode(signing_key.verifying_key().to_bytes()),
            region: "test".to_string(),
            specializations: HashSet::new(),
        };

        let governance = MockGovernance::new(
            vec![test_node],
            vec![test_version],
            "http://primary.example.com".to_string(),
            vec!["http://alt1.example.com".to_string()],
        );

        let handler = CoseHandler::new(signing_key, node_id.clone(), governance);

        let payload =
            MessagePayload::consensus_message(Bytes::from("test consensus data"), node_id);

        // Create signed message
        let cose_message = handler
            .create_signed_message(&payload, None)
            .expect("Failed to create COSE message");

        // Serialize to bytes
        let serialized = handler
            .serialize_cose_message(&cose_message)
            .expect("Failed to serialize COSE message");

        // Deserialize back
        let deserialized = handler
            .deserialize_cose_message(&serialized)
            .expect("Failed to deserialize COSE message");

        // Verify structure
        assert!(!deserialized.cose_sign1.signature.is_empty());
        assert!(deserialized.cose_sign1.payload.is_some());
    }
}
