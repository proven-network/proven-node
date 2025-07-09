//! Log types specific to local stream consensus
//!
//! This module defines the structures used for storing stream data
//! in local consensus groups.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Entry in a stream log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLogEntry {
    /// Index/sequence number of this entry
    pub index: u64,
    /// Timestamp when the entry was created
    pub timestamp: u64,
    /// The actual data
    pub data: Bytes,
    /// Metadata for the entry
    pub metadata: StreamMetadata,
}

/// Metadata for a stream log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Headers/attributes for this message
    pub headers: HashMap<String, String>,
    /// Compression type if any
    pub compression: Option<CompressionType>,
    /// Source of the message
    pub source: MessageSource,
}

/// Compression types supported
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression
    None,
    /// Gzip compression
    Gzip,
    /// Zstd compression
    Zstd,
}

/// Source of a message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageSource {
    /// Message came through consensus
    Consensus,
    /// Message came from PubSub
    PubSub {
        /// Original subject
        subject: String,
        /// Publisher node ID
        publisher: String,
    },
    /// Message was migrated from another group
    Migration {
        /// Source group ID
        source_group: u64,
    },
}
