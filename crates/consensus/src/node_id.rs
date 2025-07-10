//! Node ID type for consensus system

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

/// Node ID type for consensus system
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct NodeId(VerifyingKey);

impl NodeId {
    /// Create a new NodeId from a VerifyingKey
    pub fn new(key: VerifyingKey) -> Self {
        NodeId(key)
    }

    /// Verify a signature
    pub fn verify(
        &self,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), ed25519_dalek::SignatureError> {
        self.0.verify(message, signature)
    }

    /// Create a NodeId from bytes
    pub fn from_bytes(bytes: &[u8; 32]) -> Result<Self, ed25519_dalek::SignatureError> {
        let key = VerifyingKey::from_bytes(bytes)?;
        Ok(NodeId(key))
    }

    /// Create a NodeId from a hex string
    pub fn from_hex(hex_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let bytes = hex::decode(hex_str)?;
        if bytes.len() != 32 {
            return Err("Invalid hex string length, expected 32 bytes".into());
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(Self::from_bytes(&array)?)
    }

    /// Get the underlying VerifyingKey
    pub fn verifying_key(&self) -> &VerifyingKey {
        &self.0
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.0.to_bytes())
    }

    /// Generate a deterministic NodeId from a seed
    /// TODO: Reenable test cfg once orchestrator is refactored
    // #[cfg(any(test, feature = "test-helpers"))]
    pub fn from_seed(seed: u8) -> crate::NodeId {
        use ed25519_dalek::{SigningKey, VerifyingKey};
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        crate::NodeId::new(verifying_key)
    }
}

impl From<VerifyingKey> for NodeId {
    fn from(key: VerifyingKey) -> Self {
        NodeId(key)
    }
}

impl From<&str> for NodeId {
    fn from(hex_str: &str) -> Self {
        Self::from_hex(hex_str).expect("Invalid hex string for NodeId")
    }
}

impl From<String> for NodeId {
    fn from(hex_str: String) -> Self {
        Self::from_hex(&hex_str).expect("Invalid hex string for NodeId")
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bytes().cmp(&other.0.to_bytes())
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0.to_bytes()))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        NodeId(VerifyingKey::from_bytes(&[0; 32]).unwrap())
    }
}

impl std::hash::Hash for NodeId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bytes().hash(state);
    }
}

impl PartialEq<VerifyingKey> for NodeId {
    fn eq(&self, other: &VerifyingKey) -> bool {
        self.0 == *other
    }
}
