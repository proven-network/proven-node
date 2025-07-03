//! Messaging request and response types for consensus operations

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Messaging request for consensus operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagingRequest {
    /// Operation type
    pub operation: MessagingOperation,
}

/// Types of messaging operations that can be performed
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessagingOperation {
    /// Publish a single message directly to a stream
    PublishToStream {
        /// Target stream name
        stream: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Publish a single message with metadata directly to a stream
    PublishToStreamWithMetadata {
        /// Target stream name
        stream: String,
        /// Message data to publish
        data: Bytes,
        /// Associated metadata
        metadata: HashMap<String, String>,
    },
    /// Publish multiple messages as a batch directly to a stream
    PublishBatchToStream {
        /// Target stream name
        stream: String,
        /// List of messages to publish
        messages: Vec<Bytes>,
    },
    /// Delete a specific message by sequence number from a stream
    DeleteFromStream {
        /// Target stream name
        stream: String,
        /// Sequence number to delete
        sequence: u64,
    },
    /// Rollup operation (replace all previous messages) on a stream
    RollupStream {
        /// Target stream name
        stream: String,
        /// New rollup data
        data: Bytes,
        /// Expected current sequence number
        expected_seq: u64,
    },
    /// Publish to a subject (NEW - simplified subject publishing)
    Publish {
        /// Subject to publish to
        subject: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Publish to a subject (DEPRECATED - use Publish instead)
    PublishToSubject {
        /// Subject to publish to
        subject: String,
        /// Message data to publish
        data: Bytes,
    },
    /// Subscribe a stream to a subject pattern (NEW - consensus operation)
    SubscribeToSubject {
        /// Stream name to subscribe
        stream_name: String,
        /// Subject pattern to subscribe to  
        subject_pattern: String,
    },
    /// Unsubscribe a stream from a subject pattern (NEW - consensus operation)
    UnsubscribeFromSubject {
        /// Stream name to unsubscribe
        stream_name: String,
        /// Subject pattern to unsubscribe from
        subject_pattern: String,
    },
    /// Remove all subject subscriptions for a stream (NEW - consensus operation)
    RemoveStreamSubscriptions {
        /// Stream name to remove all subscriptions for
        stream_name: String,
    },
    /// Subscribe a stream to multiple subject patterns in one operation (NEW - bulk operation)
    BulkSubscribeToSubjects {
        /// Stream name to subscribe
        stream_name: String,
        /// List of subject patterns to subscribe to
        subject_patterns: Vec<String>,
    },
    /// Unsubscribe a stream from multiple subject patterns in one operation (NEW - bulk operation)
    BulkUnsubscribeFromSubjects {
        /// Stream name to unsubscribe
        stream_name: String,
        /// List of subject patterns to unsubscribe from
        subject_patterns: Vec<String>,
    },
}

/// Response from a messaging operation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessagingResponse {
    /// Assigned sequence number
    pub sequence: u64,
    /// Success flag
    pub success: bool,
    /// Optional error message
    pub error: Option<String>,
}
