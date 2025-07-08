//! Global consensus components
//!
//! This module contains the global consensus manager and related components
//! including state machine, storage implementations, and snapshot functionality.

pub mod global_manager;
pub mod snapshot;
pub mod state_machine;
pub mod storage;

use crate::node::Node;
use crate::node_id::NodeId;

use bytes::Bytes;
use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

// Re-export main types
pub use global_manager::{GlobalManager, PendingRequest};
pub use snapshot::SnapshotData;
pub use state_machine::{MessageData, StreamData, StreamStore};
pub use storage::{
    ConsensusStorage, MemoryConsensusStorage, RocksConsensusStorage, create_memory_storage,
    create_memory_storage_with_stream_store, create_rocks_storage,
    create_rocks_storage_with_stream_store,
};

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub GlobalTypeConfig:
        D = GlobalRequest,
        R = GlobalResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<GlobalTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Operations that can be performed through consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalOperation {
    /// Publish a message to a stream
    PublishToStream {
        /// Stream name to publish to
        stream: String,
        /// Message data
        data: Bytes,
        /// Optional metadata
        metadata: Option<std::collections::HashMap<String, String>>,
    },

    /// Publish multiple messages to a stream
    PublishBatchToStream {
        /// Stream name to publish to
        stream: String,

        /// Messages to publish
        messages: Vec<Bytes>,
    },

    /// Rollup operation on a stream
    RollupStream {
        /// Stream name to rollup
        stream: String,

        /// Message data
        data: Bytes,

        /// Expected sequence number
        expected_seq: u64,
    },

    /// Delete a message from a stream
    DeleteFromStream {
        /// Stream name to delete from
        stream: String,

        /// Sequence number of the message to delete
        sequence: u64,
    },

    /// Subscribe a stream to a subject pattern
    SubscribeToSubject {
        /// Stream name to subscribe to
        stream_name: String,

        /// Subject pattern to subscribe to
        subject_pattern: String,
    },

    /// Unsubscribe a stream from a subject pattern
    UnsubscribeFromSubject {
        /// Stream name to unsubscribe from
        stream_name: String,

        /// Subject pattern to unsubscribe from
        subject_pattern: String,
    },

    /// Remove all subscriptions for a stream
    RemoveStreamSubscriptions {
        /// Stream name to remove subscriptions from
        stream_name: String,
    },

    /// Bulk subscribe to multiple subject patterns
    BulkSubscribeToSubjects {
        /// Stream name to subscribe
        stream_name: String,
        /// Subject patterns to subscribe to
        subject_patterns: Vec<String>,
    },

    /// Bulk unsubscribe from multiple subject patterns
    BulkUnsubscribeFromSubjects {
        /// Stream name to unsubscribe
        stream_name: String,
        /// Subject patterns to unsubscribe from
        subject_patterns: Vec<String>,
    },

    /// Create a new stream with configuration
    CreateStream {
        /// Stream name to create
        stream_name: String,
        /// Stream configuration
        config: StreamConfig,
    },

    /// Update stream configuration
    UpdateStreamConfig {
        /// Stream name to update
        stream_name: String,
        /// New stream configuration
        config: StreamConfig,
    },

    /// Delete a stream
    DeleteStream {
        /// Stream name to delete
        stream_name: String,
    },

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

/// Request for consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRequest {
    /// The operation to perform
    pub operation: GlobalOperation,
}

/// Response from consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful
    pub sequence: u64,
    /// Error message if failed
    pub error: Option<String>,
}

/// Stream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Maximum number of messages to retain
    pub max_messages: Option<u64>,
    /// Maximum bytes to retain
    pub max_bytes: Option<u64>,
    /// Maximum age of messages in seconds
    pub max_age_secs: Option<u64>,
    /// Storage type for the stream
    pub storage_type: StorageType,
    /// Retention policy
    pub retention_policy: RetentionPolicy,
    /// Enable PubSub bridge for this stream
    pub pubsub_bridge_enabled: bool,
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
        }
    }
}

/// Storage type for streams
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StorageType {
    /// In-memory storage
    Memory,
    /// Persistent storage
    File,
}

/// Retention policy for streams
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Retain based on limits (age, size, count)
    Limits,
    /// Retain until explicitly acknowledged
    WorkQueue,
    /// Retain forever
    Interest,
}

/// Source information for PubSub messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessageSource {
    /// Node that published the message
    pub node_id: Option<NodeId>,
    /// Timestamp when received
    pub timestamp_secs: u64,
}
