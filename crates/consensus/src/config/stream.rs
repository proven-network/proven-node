use serde::{Deserialize, Serialize};

use crate::ConsensusGroupId;

/// Storage type for streams
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StorageType {
    /// In-memory storage
    Memory,
    /// Persistent storage
    File,
}

/// Retention policy for streams
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum RetentionPolicy {
    /// Retain based on limits (age, size, count)
    Limits,
    /// Retain until explicitly acknowledged
    WorkQueue,
    /// Retain forever
    Interest,
}

/// Compression type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CompressionType {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Zstd compression (balanced)
    Zstd,
    /// Snappy compression
    Snappy,
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Maximum number of messages to retain
    pub max_messages: Option<u64>,
    /// Maximum bytes to retain
    pub max_bytes: Option<u64>,
    /// Maximum age of messages in seconds (retention_seconds)
    pub max_age_secs: Option<u64>,
    /// Storage type for the stream
    pub storage_type: StorageType,
    /// Retention policy
    pub retention_policy: RetentionPolicy,
    /// Enable PubSub bridge for this stream
    pub pubsub_bridge_enabled: bool,
    /// Assigned consensus group (None means not yet allocated)
    pub consensus_group: Option<ConsensusGroupId>,
    /// Whether to compact on deletion
    pub compact_on_deletion: bool,
    /// Compression settings
    pub compression: CompressionType,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_messages: None,
            max_bytes: None,
            max_age_secs: None,
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: None,
            compact_on_deletion: false,
            compression: CompressionType::None,
        }
    }
}
