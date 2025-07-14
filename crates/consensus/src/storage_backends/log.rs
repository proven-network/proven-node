//! Generic log storage abstraction
//!
//! This module provides a generic log storage interface that can be used
//! for any type of log entries (Raft logs, stream logs, etc.). The specific
//! metadata and behavior is defined by the consumers of this trait.
//!
//! This trait is optimized for append-only workloads with the following features:
//! - O(1) access to first/last entries via metadata caching
//! - Batch append operations for better throughput
//! - Efficient range scans and time-based queries
//! - Built-in compaction support for space management

use super::{StorageKey, StorageNamespace, StorageResult};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

/// A generic log entry with customizable metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<M> {
    /// The index/sequence number of this entry
    pub index: u64,
    /// The timestamp when this entry was created
    pub timestamp: u64,
    /// The actual data payload
    pub data: Bytes,
    /// Generic metadata associated with the entry
    pub metadata: M,
}

/// Trait for log-oriented storage operations with generic metadata
#[async_trait]
pub trait LogStorage<M>: Send + Sync
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    /// Append a single log entry
    async fn append_entry(
        &self,
        namespace: &StorageNamespace,
        entry: LogEntry<M>,
    ) -> StorageResult<()>;

    /// Append multiple log entries in order (optimized for batch operations)
    async fn append_entries(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<LogEntry<M>>,
    ) -> StorageResult<()>;

    /// Create a batch for multiple operations
    async fn create_batch(&self) -> Box<dyn LogBatch<M>>;

    /// Apply a batch of operations atomically
    async fn apply_batch(&self, batch: Box<dyn LogBatch<M>>) -> StorageResult<()>;

    /// Read log entries within a range
    async fn read_range<R: RangeBounds<u64> + Send>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> StorageResult<Vec<LogEntry<M>>>;

    /// Get a single log entry by index
    async fn get_entry(
        &self,
        namespace: &StorageNamespace,
        index: u64,
    ) -> StorageResult<Option<LogEntry<M>>>;

    /// Get the last log entry
    async fn get_last_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry<M>>>;

    /// Get the first log entry
    async fn get_first_entry(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<LogEntry<M>>>;

    /// Truncate the log, removing all entries after the given index
    async fn truncate(&self, namespace: &StorageNamespace, after_index: u64) -> StorageResult<()>;

    /// Purge the log, removing all entries before and including the given index
    async fn purge(&self, namespace: &StorageNamespace, up_to_index: u64) -> StorageResult<()>;

    /// Get the current log state (first and last indices)
    async fn get_log_state(&self, namespace: &StorageNamespace) -> StorageResult<Option<LogState>>;

    /// Read entries within a time range
    async fn read_time_range(
        &self,
        namespace: &StorageNamespace,
        start_time: u64,
        end_time: u64,
    ) -> StorageResult<Vec<LogEntry<M>>>;

    /// Compact the log by merging old entries (implementation-specific)
    async fn compact(
        &self,
        namespace: &StorageNamespace,
        up_to_index: u64,
    ) -> StorageResult<CompactionResult>;
}

/// State of a log
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LogState {
    /// First index in the log (inclusive)
    pub first_index: u64,
    /// Last index in the log (inclusive)
    pub last_index: u64,
    /// Total number of entries
    pub entry_count: u64,
    /// Total size in bytes
    pub total_bytes: u64,
}

/// Result of a compaction operation
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Number of entries compacted
    pub entries_compacted: u64,
    /// Bytes freed by compaction
    pub bytes_freed: u64,
    /// New first index after compaction
    pub new_first_index: u64,
}

/// Batch operations for log storage
pub trait LogBatch<M>: Send
where
    M: Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    /// Add an append operation to the batch
    fn append(&mut self, namespace: StorageNamespace, entry: LogEntry<M>);

    /// Add a truncate operation to the batch
    fn truncate(&mut self, namespace: StorageNamespace, after_index: u64);

    /// Add a purge operation to the batch
    fn purge(&mut self, namespace: StorageNamespace, up_to_index: u64);
}

/// Configuration for log storage optimization
#[derive(Debug, Clone)]
pub struct LogStorageConfig {
    /// Enable metadata caching for O(1) first/last access
    pub enable_metadata_cache: bool,
    /// Batch size for bulk operations
    pub batch_size: usize,
    /// Enable compression for old entries
    pub enable_compression: bool,
    /// Compaction threshold (entries before auto-compaction)
    pub compaction_threshold: u64,
}

impl Default for LogStorageConfig {
    fn default() -> Self {
        Self {
            enable_metadata_cache: true,
            batch_size: 1000,
            enable_compression: false,
            compaction_threshold: 100_000,
        }
    }
}

/// Key encoding utilities for log entries
pub mod keys {
    use super::*;

    /// Prefix for log entries
    pub const LOG_PREFIX: &[u8] = b"log:";
    /// Prefix for metadata
    pub const META_PREFIX: &[u8] = b"meta:";

    /// Encode a log entry key using binary encoding for efficient range scans
    pub fn encode_log_key(index: u64) -> StorageKey {
        let mut key = Vec::with_capacity(LOG_PREFIX.len() + 8);
        key.extend_from_slice(LOG_PREFIX);
        key.extend_from_slice(&index.to_be_bytes());
        StorageKey(Bytes::from(key))
    }

    /// Encode a metadata key
    pub fn encode_metadata_key(key: &str) -> StorageKey {
        let mut bytes = Vec::with_capacity(META_PREFIX.len() + key.len());
        bytes.extend_from_slice(META_PREFIX);
        bytes.extend_from_slice(key.as_bytes());
        StorageKey(Bytes::from(bytes))
    }

    /// Get the start key for a log range scan
    pub fn log_range_start(start_index: u64) -> StorageKey {
        encode_log_key(start_index)
    }

    /// Get the end key for a log range scan (exclusive)
    pub fn log_range_end(end_index: Option<u64>) -> StorageKey {
        match end_index {
            Some(idx) => encode_log_key(idx),
            None => {
                // Create a key that's after all log entries
                let mut key = Vec::from(LOG_PREFIX);
                key.push(0xFF); // Max byte to ensure it's after all log entries
                StorageKey(Bytes::from(key))
            }
        }
    }

    /// Decode a log index from a key
    pub fn decode_log_index(key: &StorageKey) -> Option<u64> {
        let bytes = key.as_bytes();
        if bytes.starts_with(LOG_PREFIX) {
            let index_bytes = &bytes[LOG_PREFIX.len()..];
            if index_bytes.len() == 8 {
                let mut array = [0u8; 8];
                array.copy_from_slice(index_bytes);
                return Some(u64::from_be_bytes(array));
            }
        }
        None
    }
}
