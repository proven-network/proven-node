//! Migration-specific types for stream migration
//!
//! This module defines types used during stream migration that were previously
//! part of the hierarchical module.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Message source type for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageSourceType {
    /// Message from consensus
    Consensus,
    /// Message from pub/sub
    PubSub {
        /// Subject
        subject: String,
        /// Publisher
        publisher: String,
    },
    /// Message from migration
    Migration {
        /// Source group
        source_group: u32,
    },
}

/// Stream message for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    /// Sequence number
    pub sequence: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Message data
    pub data: Bytes,
    /// Headers
    pub headers: HashMap<String, String>,
    /// Source type
    pub source: MessageSourceType,
}

/// Migration stream metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStreamMetadata {
    /// Whether the stream is paused
    pub is_paused: bool,
    /// Timestamp when paused
    pub paused_at: Option<u64>,
    /// Whether there are pending operations
    pub has_pending_operations: bool,
}
