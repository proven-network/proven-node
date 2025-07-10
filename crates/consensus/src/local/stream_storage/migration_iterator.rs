//! Migration iterators for efficient stream data export
//!
//! This module provides iterators that can efficiently stream data from
//! storage without loading everything into memory.

use crate::error::ConsensusResult;
use crate::local::stream_storage::log_types::{
    CompressionType as StorageCompressionType, MessageSource, StreamMetadata as StreamLogMetadata,
};
use crate::migration::{MessageSourceType, StreamMessage};
use crate::storage::{StorageEngine, StorageNamespace, log::LogStorage};
use std::sync::Arc;

/// Iterator for streaming messages from storage during migration
pub struct MigrationMessageIterator<S: StorageEngine + LogStorage<StreamLogMetadata>> {
    /// Storage instance
    storage: Arc<S>,
    /// Namespace to read from
    namespace: StorageNamespace,
    /// Start sequence (inclusive)
    #[allow(dead_code)]
    start_seq: u64,
    /// End sequence (inclusive)
    end_seq: u64,
    /// Current sequence
    current_seq: u64,
    /// Running checksum
    checksum_hasher: sha2::Sha256,
    /// Total messages processed
    message_count: u64,
    /// Total bytes processed
    total_bytes: u64,
}

impl<S: StorageEngine + LogStorage<StreamLogMetadata>> MigrationMessageIterator<S> {
    /// Create a new migration iterator
    pub async fn new(
        storage: Arc<S>,
        namespace: StorageNamespace,
        start_seq: u64,
        end_seq: u64,
    ) -> ConsensusResult<Self> {
        use sha2::Digest;

        Ok(Self {
            storage,
            namespace,
            start_seq,
            end_seq,
            current_seq: start_seq,
            checksum_hasher: sha2::Sha256::new(),
            message_count: 0,
            total_bytes: 0,
        })
    }

    /// Get the next message from storage
    pub async fn next_message(&mut self) -> ConsensusResult<Option<StreamMessage>> {
        use sha2::Digest;

        // Loop through sequences until we find a message or reach the end
        while self.current_seq <= self.end_seq {
            // Use LogStorage to get the entry at the current sequence
            match self
                .storage
                .get_entry(&self.namespace, self.current_seq)
                .await
                .map_err(|_e| {
                    crate::error::Error::Storage(crate::error::StorageError::OperationFailed {
                        operation: "get_entry".to_string(),
                    })
                })? {
                Some(log_entry) => {
                    // Update checksum with raw data
                    self.checksum_hasher.update(log_entry.data.as_ref());
                    self.message_count += 1;
                    self.total_bytes += log_entry.data.len() as u64;

                    // Convert to migration message
                    let message = StreamMessage {
                        sequence: log_entry.index,
                        timestamp: log_entry.timestamp,
                        data: log_entry.data,
                        headers: log_entry.metadata.headers,
                        compression: log_entry.metadata.compression.map(|c| match c {
                            StorageCompressionType::None => "none".to_string(),
                            StorageCompressionType::Gzip => "gzip".to_string(),
                            StorageCompressionType::Zstd => "zstd".to_string(),
                        }),
                        source: match log_entry.metadata.source {
                            MessageSource::Consensus => MessageSourceType::Consensus,
                            MessageSource::PubSub { subject, publisher } => {
                                MessageSourceType::PubSub { subject, publisher }
                            }
                            MessageSource::Migration { source_group } => {
                                MessageSourceType::Migration { source_group }
                            }
                        },
                    };

                    self.current_seq += 1;
                    return Ok(Some(message));
                }
                None => {
                    // No entry at this sequence, try the next one
                    self.current_seq += 1;
                }
            }
        }

        Ok(None)
    }

    /// Get the current checksum
    pub fn checksum(&self) -> String {
        use sha2::Digest;
        format!("{:x}", self.checksum_hasher.clone().finalize())
    }

    /// Get the number of messages processed
    pub fn message_count(&self) -> u64 {
        self.message_count
    }

    /// Get the total bytes processed
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Create chunks of messages for streaming
    pub async fn next_chunk(
        &mut self,
        chunk_size: usize,
    ) -> ConsensusResult<Option<Vec<StreamMessage>>> {
        let mut chunk = Vec::with_capacity(chunk_size);

        for _ in 0..chunk_size {
            match self.next_message().await? {
                Some(msg) => chunk.push(msg),
                None => break,
            }
        }

        if chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

/// Configuration for migration export
pub struct MigrationExportConfig {
    /// Chunk size for streaming
    pub chunk_size: usize,
    /// Whether to compress chunks
    pub compress_chunks: bool,
    /// Include pending operations
    pub include_pending: bool,
}

impl Default for MigrationExportConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1000,
            compress_chunks: true,
            include_pending: true,
        }
    }
}
