//! Stream configuration types

use serde::{Deserialize, Serialize};

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
