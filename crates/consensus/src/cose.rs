//! COSE (CBOR Object Signing and Encryption) support for secure messaging

use bytes::Bytes;
use coset::{
    CoseSign1, CoseSign1Builder, HeaderBuilder, Label, TaggedCborSerializable, cbor::value::Value,
    iana,
};
use ed25519_dalek::{Signature, Signer, SigningKey};
use uuid::Uuid;

use crate::{NodeId, error::Error};

/// COSE-signed message wrapper
#[derive(Debug, Clone)]
pub struct CoseMessage {
    /// The COSE_Sign1 object containing the signed message
    pub cose_sign1: CoseSign1,
}

/// Extracted metadata from COSE protected headers
#[derive(Debug, Clone)]
pub struct CoseMetadata {
    /// Message timestamp from protected headers
    pub timestamp: Option<u64>,
    /// Correlation ID from protected headers (for request/response correlation)
    pub correlation_id: Option<Uuid>,
    /// Message type from protected headers
    pub message_type: Option<String>,
}

/// COSE message handler for creating and verifying signed messages
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
    ///
    /// # Arguments
    /// * `message_data` - The serialized message bytes to sign
    /// * `message_type` - Type identifier for the message (goes in protected headers)
    /// * `correlation_id` - Optional correlation ID for request/response tracking
    pub fn create_signed_message(
        &self,
        message_data: &[u8],
        message_type: &str,
        correlation_id: Option<Uuid>,
    ) -> Result<CoseMessage, Error> {
        // Build protected headers with algorithm
        let protected_header = HeaderBuilder::new()
            .algorithm(iana::Algorithm::EdDSA)
            .build();

        // Build unprotected headers with custom data
        let mut unprotected_builder = HeaderBuilder::new();

        // Add message type
        unprotected_builder = unprotected_builder.text_value(
            "msg_type".to_string(),
            Value::Text(message_type.to_string()),
        );

        // Add timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        unprotected_builder = unprotected_builder
            .text_value("timestamp".to_string(), Value::Integer(timestamp.into()));

        // Add correlation ID if provided
        if let Some(correlation_id) = correlation_id {
            unprotected_builder = unprotected_builder.text_value(
                "correlation_id".to_string(),
                Value::Text(correlation_id.to_string()),
            );
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

        Ok(CoseMessage {
            cose_sign1: final_sign1,
        })
    }

    /// Verify a COSE_Sign1 message and extract the payload and metadata
    pub async fn verify_signed_message(
        &self,
        cose_message: &CoseMessage,
        sender_node_id: NodeId,
    ) -> Result<(Bytes, CoseMetadata), Error> {
        let sign1 = &cose_message.cose_sign1;

        // Verify the signature
        let to_verify = sign1.tbs_data(b"");
        let signature_bytes = if sign1.signature.is_empty() {
            return Err(Error::InvalidMessage("Missing signature".to_string()));
        } else {
            &sign1.signature
        };

        let signature = Signature::from_bytes(
            signature_bytes
                .as_slice()
                .try_into()
                .map_err(|_| Error::InvalidMessage("Invalid signature length".to_string()))?,
        );

        sender_node_id
            .verify(&to_verify, &signature)
            .map_err(|e| Error::InvalidMessage(format!("Signature verification failed: {e}")))?;

        // Extract payload
        let payload_bytes = sign1
            .payload
            .as_ref()
            .ok_or_else(|| Error::InvalidMessage("Missing payload".to_string()))?;

        // Extract metadata from headers
        let metadata = self.extract_metadata_from_headers(sign1)?;

        Ok((Bytes::from(payload_bytes.clone()), metadata))
    }

    /// Extract metadata from COSE headers (both protected and unprotected)
    fn extract_metadata_from_headers(&self, sign1: &CoseSign1) -> Result<CoseMetadata, Error> {
        let mut timestamp = None;
        let mut correlation_id = None;
        let mut message_type = None;

        // Extract from unprotected headers (where we store our custom data)
        for (label, value) in &sign1.unprotected.rest {
            if let Label::Text(key) = label {
                match key.as_str() {
                    "timestamp" => {
                        if let Value::Integer(ts) = value {
                            // Convert ciborium Integer to u64
                            if let Ok(ts_u64) = u64::try_from(*ts) {
                                timestamp = Some(ts_u64);
                            }
                        }
                    }
                    "msg_type" => {
                        if let Value::Text(mt) = value {
                            message_type = Some(mt.clone());
                        }
                    }
                    "correlation_id" => {
                        if let Value::Text(cid) = value {
                            if let Ok(uuid) = Uuid::parse_str(cid) {
                                correlation_id = Some(uuid);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(CoseMetadata {
            timestamp,
            correlation_id,
            message_type,
        })
    }

    /// Serialize a COSE message to bytes for transmission
    pub fn serialize_cose_message(&self, cose_message: &CoseMessage) -> Result<Vec<u8>, Error> {
        let cbor_bytes = cose_message
            .cose_sign1
            .clone()
            .to_tagged_vec()
            .map_err(|e| Error::InvalidMessage(format!("COSE serialization failed: {e}")))?;

        Ok(cbor_bytes)
    }

    /// Deserialize bytes into a COSE message
    pub fn deserialize_cose_message(&self, bytes: &[u8]) -> Result<CoseMessage, Error> {
        let cose_sign1 = CoseSign1::from_tagged_slice(bytes)
            .map_err(|e| Error::InvalidMessage(format!("COSE deserialization failed: {e}")))?;

        Ok(CoseMessage { cose_sign1 })
    }
}

impl std::fmt::Debug for CoseHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CoseHandler")
    }
}
