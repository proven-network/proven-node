//! Core type definitions for the consensus system

use bytes::Bytes;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use openraft::Entry;
use proven_governance::GovernanceNode;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

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

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub TypeConfig:
        D = MessagingRequest,
        R = MessagingResponse,
        NodeId = NodeId,
        Node = GovernanceNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Operations that can be performed through consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagingOperation {
    /// Publish a message to a stream
    PublishToStream {
        /// Stream name to publish to
        stream: String,
        /// Message data
        data: Bytes,
    },

    /// Publish a message with metadata to a stream
    PublishToStreamWithMetadata {
        /// Stream name to publish to
        stream: String,

        /// Message data
        data: Bytes,

        /// Metadata
        metadata: std::collections::HashMap<String, String>,
    },

    /// Publish multiple messages to a stream
    PublishBatchToStream {
        /// Stream name to publish to
        stream: String,

        /// Messages to publish
        messages: Vec<Bytes>,
    },
    /// Publish a message to a subject
    Publish {
        /// Subject to publish to
        subject: String,

        /// Message data
        data: Bytes,
    },

    /// Publish a message with metadata to a subject
    PublishWithMetadata {
        /// Subject to publish to
        subject: String,

        /// Message data
        data: Bytes,

        /// Metadata
        metadata: std::collections::HashMap<String, String>,
    },

    /// Rollup operation on a stream
    RollupStream {
        /// Stream name to rollup
        stream: String,

        /// Message data
        data: Bytes,

        /// Expected sequence number
        expected_seq: u64,
    },

    /// Delete a message from a stream
    DeleteFromStream {
        /// Stream name to delete from
        stream: String,

        /// Sequence number of the message to delete
        sequence: u64,
    },

    /// Subscribe a stream to a subject pattern
    SubscribeToSubject {
        /// Stream name to subscribe to
        stream_name: String,

        /// Subject pattern to subscribe to
        subject_pattern: String,
    },

    /// Unsubscribe a stream from a subject pattern
    UnsubscribeFromSubject {
        /// Stream name to unsubscribe from
        stream_name: String,

        /// Subject pattern to unsubscribe from
        subject_pattern: String,
    },

    /// Remove all subscriptions for a stream
    RemoveStreamSubscriptions {
        /// Stream name to remove subscriptions from
        stream_name: String,
    },

    /// Bulk subscribe to multiple subject patterns
    BulkSubscribeToSubjects {
        /// Stream name to subscribe
        stream_name: String,
        /// Subject patterns to subscribe to
        subject_patterns: Vec<String>,
    },

    /// Bulk unsubscribe from multiple subject patterns
    BulkUnsubscribeFromSubjects {
        /// Stream name to unsubscribe
        stream_name: String,
        /// Subject patterns to unsubscribe from
        subject_patterns: Vec<String>,
    },
}

/// Request for consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingRequest {
    /// The operation to perform
    pub operation: MessagingOperation,
}

/// Response from consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful
    pub sequence: u64,
    /// Error message if failed
    pub error: Option<String>,
}
