//! COSE (CBOR Object Signing and Encryption) support for secure messaging

use crate::connection_verifier::{VerificationError, VerificationResult};
use bytes::Bytes;
pub use coset::CoseSign1;
use coset::{
    CoseSign1Builder, HeaderBuilder, Label, TaggedCborSerializable, cbor::value::Value, iana,
};
use ed25519_dalek::{Signature, Signer, SigningKey};
use proven_topology::NodeId;
use std::collections::HashMap;

/// COSE message handler for creating and verifying signed messages
#[derive(Debug)]
pub struct CoseHandler {
    /// Node's signing key
    signing_key: SigningKey,
}

impl CoseHandler {
    /// Create a new COSE handler
    pub const fn new(signing_key: SigningKey) -> Self {
        Self { signing_key }
    }

    /// Create a COSE_Sign1 message with ed25519 signature
    pub fn create_signed_message(
        &self,
        message_data: &[u8],
        headers: &HashMap<String, String>,
    ) -> VerificationResult<CoseSign1> {
        // Build protected headers with algorithm
        let protected_header = HeaderBuilder::new()
            .algorithm(iana::Algorithm::EdDSA)
            .build();

        // Build unprotected headers with custom data
        let mut unprotected_builder = HeaderBuilder::new();

        // Add timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        unprotected_builder = unprotected_builder
            .text_value("timestamp".to_string(), Value::Integer(timestamp.into()));

        // Add all provided headers
        for (key, value) in headers {
            unprotected_builder =
                unprotected_builder.text_value(key.clone(), Value::Text(value.clone()));
        }

        let unprotected_header = unprotected_builder.build();

        // Create COSE_Sign1 structure with unprotected headers
        let sign1_builder = CoseSign1Builder::new()
            .protected(protected_header)
            .unprotected(unprotected_header)
            .payload(message_data.to_vec());

        // Create signature using the create_signature method
        let final_sign1 = sign1_builder
            .create_signature(b"", |pt| {
                let signature = self.signing_key.sign(pt);
                signature.to_bytes().to_vec()
            })
            .build();

        Ok(final_sign1)
    }

    /// Verify a COSE_Sign1 message and extract the payload and headers
    pub fn verify_signed_message(
        &self,
        sign1: &CoseSign1,
        sender_node_id: &NodeId,
    ) -> VerificationResult<(Bytes, HashMap<String, String>)> {
        // Verify the signature
        let to_verify = sign1.tbs_data(b"");
        let signature_bytes = if sign1.signature.is_empty() {
            return Err(VerificationError::InvalidMessage(
                "Missing signature".to_string(),
            ));
        } else {
            &sign1.signature
        };

        let signature =
            Signature::from_bytes(signature_bytes.as_slice().try_into().map_err(|_| {
                VerificationError::InvalidMessage("Invalid signature length".to_string())
            })?);

        sender_node_id.verify(&to_verify, &signature).map_err(|e| {
            VerificationError::SignatureVerification(format!("Signature verification failed: {e}"))
        })?;

        // Extract payload
        let payload_bytes = sign1
            .payload
            .as_ref()
            .ok_or_else(|| VerificationError::InvalidMessage("Missing payload".to_string()))?;

        // Extract headers from unprotected headers
        let mut headers = HashMap::new();
        for (label, value) in &sign1.unprotected.rest {
            if let Label::Text(key) = label {
                match value {
                    Value::Text(text) => {
                        headers.insert(key.clone(), text.clone());
                    }
                    Value::Integer(int) => {
                        // Convert integer to string
                        if let Ok(u64_val) = u64::try_from(*int) {
                            headers.insert(key.clone(), u64_val.to_string());
                        } else if let Ok(i64_val) = i64::try_from(*int) {
                            headers.insert(key.clone(), i64_val.to_string());
                        }
                    }
                    _ => {} // Skip other value types
                }
            }
        }

        Ok((Bytes::from(payload_bytes.clone()), headers))
    }

    /// Serialize a COSE message to bytes for transmission
    pub fn serialize_cose_message(&self, cose_sign1: &CoseSign1) -> VerificationResult<Bytes> {
        let cbor_bytes = cose_sign1.clone().to_tagged_vec().map_err(|e| {
            VerificationError::InvalidMessage(format!("COSE serialization failed: {e}"))
        })?;

        Ok(Bytes::from(cbor_bytes))
    }

    /// Deserialize bytes into a COSE message
    pub fn deserialize_cose_message(&self, bytes: &Bytes) -> VerificationResult<CoseSign1> {
        let cose_sign1 = CoseSign1::from_tagged_slice(bytes.as_ref()).map_err(|e| {
            VerificationError::InvalidMessage(format!("COSE deserialization failed: {e}"))
        })?;

        Ok(cose_sign1)
    }
}
