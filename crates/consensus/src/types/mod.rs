//! Core type definitions for the consensus system

pub mod node;
pub mod node_id;

pub use node::Node;
pub use node_id::NodeId;

use bytes::Bytes;
use openraft::Entry;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

openraft::declare_raft_types!(
    /// Types for the application using RaftTypeConfig
    pub TypeConfig:
        D = MessagingRequest,
        R = MessagingResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Operations that can be performed through consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagingOperation {
    /// Publish a message to a stream
    PublishToStream {
        /// Stream name to publish to
        stream: String,
        /// Message data
        data: Bytes,
    },

    /// Publish a message with metadata to a stream
    PublishToStreamWithMetadata {
        /// Stream name to publish to
        stream: String,

        /// Message data
        data: Bytes,

        /// Metadata
        metadata: std::collections::HashMap<String, String>,
    },

    /// Publish multiple messages to a stream
    PublishBatchToStream {
        /// Stream name to publish to
        stream: String,

        /// Messages to publish
        messages: Vec<Bytes>,
    },
    /// Publish a message to a subject
    Publish {
        /// Subject to publish to
        subject: String,

        /// Message data
        data: Bytes,
    },

    /// Publish a message with metadata to a subject
    PublishWithMetadata {
        /// Subject to publish to
        subject: String,

        /// Message data
        data: Bytes,

        /// Metadata
        metadata: std::collections::HashMap<String, String>,
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
}

/// Request for consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingRequest {
    /// The operation to perform
    pub operation: MessagingOperation,
}

/// Response from consensus operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingResponse {
    /// Whether the operation succeeded
    pub success: bool,
    /// Sequence number if successful
    pub sequence: u64,
    /// Error message if failed
    pub error: Option<String>,
}
