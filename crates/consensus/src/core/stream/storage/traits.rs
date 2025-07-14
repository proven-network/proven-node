//! Storage traits and types for stream-oriented operations
//!
//! This module defines the core traits and types for stream storage operations,
//! particularly focused on migration-friendly designs that allow efficient
//! export/import of streams between consensus groups.

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::config::StreamConfig;
use crate::core::group::migration::MigrationCheckpoint;
use crate::error::ConsensusResult;

/// Core trait for stream-oriented storage operations
#[async_trait]
pub trait StreamStorage: Send + Sync {
    /// Export entire stream to a portable format
    async fn export_stream(&self, stream_name: &str) -> ConsensusResult<StreamExport>;

    /// Import stream from export
    async fn import_stream(&self, export: StreamExport) -> ConsensusResult<()>;

    /// Get checkpoint without locking the stream
    async fn create_checkpoint(
        &self,
        stream_name: &str,
        since_seq: Option<u64>,
    ) -> ConsensusResult<MigrationCheckpoint>;

    /// Apply checkpoint to restore stream state
    async fn apply_checkpoint(&self, checkpoint: &MigrationCheckpoint) -> ConsensusResult<()>;

    /// Get storage metrics for a stream
    async fn get_stream_metrics(&self, stream_name: &str) -> ConsensusResult<StreamMetrics>;

    /// Check if a stream exists
    async fn stream_exists(&self, stream_name: &str) -> ConsensusResult<bool>;

    /// Delete a stream and all its data
    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<()>;
}

/// Portable stream export format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamExport {
    /// Stream metadata
    pub metadata: StreamMetadata,
    /// All messages in the stream
    pub messages: Vec<(u64, MessageData)>,
    /// Format of the checkpoint
    pub checkpoint_format: CheckpointFormat,
    /// Compression type used
    pub compression: crate::config::stream::CompressionType,
    /// Export timestamp
    pub exported_at: u64,
    /// Source group ID
    pub source_group: crate::ConsensusGroupId,
}

/// Stream metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Stream name
    pub name: String,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub modified_at: u64,
    /// Current sequence number
    pub last_sequence: u64,
    /// Total message count
    pub message_count: u64,
    /// Stream configuration
    pub config: StreamConfig,
    /// Whether stream is paused
    pub is_paused: bool,
    /// Custom metadata
    pub custom_metadata: HashMap<String, String>,
}

/// Message data (compatible with state machine)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    /// Message payload
    pub data: Bytes,
    /// Message metadata
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp when message was received
    pub timestamp: u64,
}

/// Checkpoint format
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum CheckpointFormat {
    /// Full snapshot of all data
    Full,
    /// Incremental changes since a base
    Incremental {
        /// Base sequence number
        base_seq: u64,
    },
    /// Compressed format
    Compressed,
}

/// Stream pause state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseState {
    /// When the stream was paused
    pub paused_at: u64,
    /// Last sequence before pause
    pub last_seq: u64,
    /// Reason for pause
    pub reason: PauseReason,
}

/// Reason for pausing a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PauseReason {
    /// Paused for migration
    Migration {
        /// Target group for migration
        target_group: crate::ConsensusGroupId,
    },
    /// Paused for maintenance
    Maintenance,
    /// Paused manually
    Manual,
}

/// Stream storage metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetrics {
    /// Stream name
    pub stream_name: String,
    /// Disk usage in bytes
    pub disk_usage_bytes: u64,
    /// Number of messages
    pub message_count: u64,
    /// Oldest message timestamp
    pub oldest_message_timestamp: Option<u64>,
    /// Newest message timestamp
    pub newest_message_timestamp: Option<u64>,
    /// Write rate (messages per second)
    pub write_rate: f64,
    /// Read rate (messages per second)
    pub read_rate: f64,
}

/// Incremental checkpoint for efficient updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalCheckpoint {
    /// Stream name
    pub stream_name: String,
    /// Base sequence this builds on
    pub base_seq: u64,
    /// New messages since base
    pub messages: Vec<(u64, Vec<u8>)>,
    /// Whether data is compressed
    pub compressed: bool,
    /// Checksum for validation
    pub checksum: Option<u64>,
}

/// Storage-specific error types
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Stream not found
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// Stream already exists
    #[error("Stream already exists: {0}")]
    StreamExists(String),

    /// Invalid checkpoint
    #[error("Invalid checkpoint: {0}")]
    InvalidCheckpoint(String),

    /// Column family error
    #[error("Column family error: {0}")]
    ColumnFamilyError(String),

    /// RocksDB error
    #[error("RocksDB error: {0}")]
    RocksDbError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Options for stream storage
#[derive(Debug, Clone)]
pub struct StreamOptions {
    /// TTL in seconds (None = no TTL)
    pub ttl_seconds: Option<u64>,
    /// Compact on deletion
    pub compact_on_deletion: bool,
    /// Compression type
    pub compression: crate::config::stream::CompressionType,
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Max write buffer number
    pub max_write_buffer_number: i32,
    /// Target file size base in bytes
    pub target_file_size_base: u64,
    /// Enable statistics
    pub enable_statistics: bool,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: None,
            compact_on_deletion: true,
            compression: crate::config::stream::CompressionType::Lz4,
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            enable_statistics: false,
        }
    }
}

/// WAL configuration for durability vs performance trade-offs
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Sync on every commit
    pub sync_on_commit: bool,
    /// Maximum WAL size in bytes
    pub max_wal_size: usize,
    /// WAL TTL in seconds
    pub wal_ttl_seconds: u64,
    /// WAL size limit in MB
    pub wal_size_limit_mb: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            sync_on_commit: true,
            max_wal_size: 1024 * 1024 * 1024, // 1GB
            wal_ttl_seconds: 3600,            // 1 hour
            wal_size_limit_mb: 1024,          // 1GB
        }
    }
}
