//! Stream-related data models

use bytes::Bytes;
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::foundation::Message;
use crate::foundation::types::{ConsensusGroupId, StreamName};

/// Stream persistence configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistenceType {
    /// Data is stored in memory only
    Ephemeral,
    /// Data is persisted to storage
    Persistent,
}

impl Default for PersistenceType {
    fn default() -> Self {
        Self::Persistent
    }
}

/// Stream retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Retain messages for a specific duration
    Time {
        /// Duration in seconds
        seconds: u64,
    },
    /// Retain up to a specific number of messages
    Count {
        /// Maximum number of messages
        max_messages: u64,
    },
    /// Retain up to a specific size
    Size {
        /// Maximum size in bytes
        max_bytes: u64,
    },
    /// Retain forever
    Forever,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::Time {
            seconds: 7 * 24 * 60 * 60, // 7 days
        }
    }
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Retention policy
    pub retention: RetentionPolicy,
    /// Persistence type
    pub persistence_type: PersistenceType,
    /// Whether to allow auto-creation of topics
    pub allow_auto_create: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            retention: RetentionPolicy::default(),
            persistence_type: PersistenceType::Persistent,
            allow_auto_create: false,
        }
    }
}

/// Stream information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    /// Stream name
    pub name: StreamName,
    /// Stream configuration
    pub config: StreamConfig,
    /// Assigned consensus group
    pub group_id: ConsensusGroupId,
    /// Creation timestamp
    pub created_at: u64,
}

/// State for a single stream
#[derive(Debug, Clone)]
pub struct StreamState {
    /// Stream name
    pub name: StreamName,
    /// Next sequence number
    pub next_sequence: LogIndex,
    /// First sequence (for trimmed streams)
    pub first_sequence: LogIndex,
    /// Stream statistics
    pub stats: StreamStats,
}

/// Stream statistics
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total messages
    pub message_count: u64,
    /// Total bytes
    pub total_bytes: u64,
    /// Last update timestamp
    pub last_update: u64,
}

/// Stored message with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Sequence number
    pub sequence: LogIndex,
    /// Message data
    pub data: Message,
    /// Timestamp
    pub timestamp: u64,
}

/// Serialize a StoredMessage to bytes
pub fn serialize_stored_message(msg: &StoredMessage) -> Result<Bytes, String> {
    serde_json::to_vec(msg)
        .map(Bytes::from)
        .map_err(|e| format!("Failed to serialize message: {e}"))
}

/// Deserialize a StoredMessage from bytes
pub fn deserialize_stored_message(bytes: Bytes) -> Result<StoredMessage, String> {
    serde_json::from_slice(&bytes).map_err(|e| format!("Failed to deserialize message: {e}"))
}
