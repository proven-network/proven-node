//! Local stream operations for consensus groups
//!
//! This module defines all operations that can be performed on local
//! consensus groups, including stream data operations and migrations.

use crate::allocation::ConsensusGroupId;
use crate::global::PubSubMessageSource;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Local stream operations handled by local consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LocalStreamOperation {
    /// Stream data operations
    Stream(StreamOperation),
    /// Migration operations
    Migration(MigrationOperation),
    /// PubSub operations
    PubSub(PubSubOperation),
    /// Maintenance operations
    Maintenance(MaintenanceOperation),
}

impl LocalStreamOperation {
    /// Get the category of the operation
    pub fn category(&self) -> &'static str {
        match self {
            Self::Stream(_) => "stream",
            Self::Migration(_) => "migration",
            Self::PubSub(_) => "pubsub",
            Self::Maintenance(_) => "maintenance",
        }
    }

    /// Get the operation name
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::Stream(op) => op.operation_name(),
            Self::Migration(op) => op.operation_name(),
            Self::PubSub(op) => op.operation_name(),
            Self::Maintenance(op) => op.operation_name(),
        }
    }

    /// Create a publish to stream operation
    pub fn publish_to_stream(
        stream: String,
        data: Bytes,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self::Stream(StreamOperation::Publish {
            stream,
            data,
            metadata,
        })
    }

    /// Create a publish batch to stream operation
    pub fn publish_batch_to_stream(stream: String, messages: Vec<Bytes>) -> Self {
        Self::Stream(StreamOperation::PublishBatch { stream, messages })
    }

    /// Create a rollup stream operation
    pub fn rollup_stream(stream: String, data: Bytes, expected_seq: u64) -> Self {
        Self::Stream(StreamOperation::Rollup {
            stream,
            data,
            expected_seq,
        })
    }

    /// Create a delete from stream operation
    pub fn delete_from_stream(stream: String, sequence: u64) -> Self {
        Self::Stream(StreamOperation::Delete { stream, sequence })
    }

    /// Create a publish from pubsub operation
    pub fn publish_from_pubsub(
        stream_name: String,
        subject: String,
        data: Bytes,
        source: PubSubMessageSource,
    ) -> Self {
        Self::PubSub(PubSubOperation::PublishFromPubSub {
            stream_name,
            subject,
            data,
            source,
        })
    }

    /// Create a create stream for migration operation
    pub fn create_stream_for_migration(
        stream_name: String,
        source_group: ConsensusGroupId,
    ) -> Self {
        Self::Migration(MigrationOperation::CreateStream {
            stream_name,
            source_group,
        })
    }

    /// Create a get stream checkpoint operation
    pub fn get_stream_checkpoint(stream_name: String) -> Self {
        Self::Migration(MigrationOperation::GetCheckpoint { stream_name })
    }

    /// Create a get incremental checkpoint operation
    pub fn get_incremental_checkpoint(stream_name: String, since_sequence: u64) -> Self {
        Self::Migration(MigrationOperation::GetIncrementalCheckpoint {
            stream_name,
            since_sequence,
        })
    }

    /// Create an apply migration checkpoint operation
    pub fn apply_migration_checkpoint(checkpoint: Bytes) -> Self {
        Self::Migration(MigrationOperation::ApplyCheckpoint { checkpoint })
    }

    /// Create an apply incremental checkpoint operation
    pub fn apply_incremental_checkpoint(checkpoint: Bytes) -> Self {
        Self::Migration(MigrationOperation::ApplyIncrementalCheckpoint { checkpoint })
    }

    /// Create a pause stream operation
    pub fn pause_stream(stream_name: String) -> Self {
        Self::Migration(MigrationOperation::PauseStream { stream_name })
    }

    /// Create a resume stream operation
    pub fn resume_stream(stream_name: String) -> Self {
        Self::Migration(MigrationOperation::ResumeStream { stream_name })
    }

    /// Create a remove stream operation
    pub fn remove_stream(stream_name: String) -> Self {
        Self::Migration(MigrationOperation::RemoveStream { stream_name })
    }

    /// Create a get metrics operation
    pub fn get_metrics() -> Self {
        Self::Maintenance(MaintenanceOperation::GetMetrics)
    }

    /// Create a cleanup pending operations operation
    pub fn cleanup_pending_operations(max_age_secs: u64) -> Self {
        Self::Maintenance(MaintenanceOperation::CleanupPendingOperations { max_age_secs })
    }
}

/// Stream data operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOperation {
    /// Publish a message to a stream
    Publish {
        /// Stream name to publish to
        stream: String,
        /// Message data
        data: Bytes,
        /// Optional metadata
        metadata: Option<HashMap<String, String>>,
    },
    /// Publish multiple messages to a stream
    PublishBatch {
        /// Stream name to publish to
        stream: String,
        /// Messages to publish
        messages: Vec<Bytes>,
    },
    /// Rollup operation on a stream
    Rollup {
        /// Stream name to rollup
        stream: String,
        /// Message data
        data: Bytes,
        /// Expected sequence number
        expected_seq: u64,
    },
    /// Delete a message from a stream
    Delete {
        /// Stream name to delete from
        stream: String,
        /// Sequence number of the message to delete
        sequence: u64,
    },
}

impl StreamOperation {
    /// Get the operation name
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::PublishBatch { .. } => "publish_batch",
            Self::Rollup { .. } => "rollup",
            Self::Delete { .. } => "delete",
        }
    }

    /// Get the stream name for this operation
    pub fn stream_name(&self) -> &str {
        match self {
            Self::Publish { stream, .. } => stream,
            Self::PublishBatch { stream, .. } => stream,
            Self::Rollup { stream, .. } => stream,
            Self::Delete { stream, .. } => stream,
        }
    }
}

/// Migration operations for stream migrations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationOperation {
    /// Create a stream for migration
    CreateStream {
        /// Stream name to create
        stream_name: String,
        /// Source consensus group
        source_group: ConsensusGroupId,
    },
    /// Get a checkpoint of stream data for migration
    GetCheckpoint {
        /// Stream name to checkpoint
        stream_name: String,
    },
    /// Get an incremental checkpoint of stream data since a sequence number
    GetIncrementalCheckpoint {
        /// Stream name to checkpoint
        stream_name: String,
        /// Only include messages after this sequence number
        since_sequence: u64,
    },
    /// Apply a migration checkpoint
    ApplyCheckpoint {
        /// Checkpoint data
        checkpoint: Bytes,
    },
    /// Apply an incremental migration checkpoint (merges with existing data)
    ApplyIncrementalCheckpoint {
        /// Checkpoint data
        checkpoint: Bytes,
    },
    /// Pause stream for migration
    PauseStream {
        /// Stream name to pause
        stream_name: String,
    },
    /// Resume stream after migration
    ResumeStream {
        /// Stream name to resume
        stream_name: String,
    },
    /// Remove stream after migration
    RemoveStream {
        /// Stream name to remove
        stream_name: String,
    },
}

impl MigrationOperation {
    /// Get the operation name
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::CreateStream { .. } => "create_stream",
            Self::GetCheckpoint { .. } => "get_checkpoint",
            Self::GetIncrementalCheckpoint { .. } => "get_incremental_checkpoint",
            Self::ApplyCheckpoint { .. } => "apply_checkpoint",
            Self::ApplyIncrementalCheckpoint { .. } => "apply_incremental_checkpoint",
            Self::PauseStream { .. } => "pause_stream",
            Self::ResumeStream { .. } => "resume_stream",
            Self::RemoveStream { .. } => "remove_stream",
        }
    }
}

/// PubSub operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubOperation {
    /// Publish from PubSub to a stream
    PublishFromPubSub {
        /// Stream name to publish to
        stream_name: String,
        /// Subject that triggered this
        subject: String,
        /// Message data
        data: Bytes,
        /// Source information
        source: PubSubMessageSource,
    },
}

impl PubSubOperation {
    /// Get the operation name
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::PublishFromPubSub { .. } => "publish_from_pubsub",
        }
    }
}

/// Maintenance operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaintenanceOperation {
    /// Get metrics for the local state machine
    GetMetrics,
    /// Clean up old pending operations
    CleanupPendingOperations {
        /// Maximum age of pending operations to keep (in seconds)
        max_age_secs: u64,
    },
}

impl MaintenanceOperation {
    /// Get the operation name
    pub fn operation_name(&self) -> &'static str {
        match self {
            Self::GetMetrics => "get_metrics",
            Self::CleanupPendingOperations { .. } => "cleanup_pending_operations",
        }
    }
}
