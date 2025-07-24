//! Stream-related data models

use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::{StreamConfig, StreamName};

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
