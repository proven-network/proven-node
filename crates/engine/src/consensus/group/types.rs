//! Types for group consensus layer

use std::{num::NonZero, sync::Arc};

use serde::{Deserialize, Serialize};

/// Message type for consensus operations
pub type MessageData = crate::foundation::Message;
use crate::services::stream::StreamName;

/// Group consensus request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupRequest {
    /// Stream operation
    Stream(StreamOperation),
    /// Administrative operation
    Admin(AdminOperation),
}

/// Stream operation within a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOperation {
    /// Append messages to stream
    Append {
        /// Stream name
        stream: StreamName,
        /// Messages to append
        messages: Vec<MessageData>,
        /// Timestamp assigned by the leader (milliseconds since epoch)
        timestamp: u64,
    },
    /// Trim stream
    Trim {
        /// Stream name
        stream: StreamName,
        /// Trim up to this sequence
        up_to_seq: NonZero<u64>,
    },
    /// Delete a specific message from stream
    Delete {
        /// Stream name
        stream: StreamName,
        /// Sequence number to delete
        sequence: NonZero<u64>,
    },
}

/// Administrative operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminOperation {
    /// Initialize stream in this group
    InitializeStream {
        /// Stream name
        stream: StreamName,
    },
    /// Remove stream from this group
    RemoveStream {
        /// Stream name
        stream: StreamName,
    },
    /// Compact storage
    Compact,
}

/// Group consensus response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GroupResponse {
    /// Operation succeeded
    Success,
    /// Message appended
    Appended {
        /// Stream name
        stream: StreamName,
        /// Assigned sequence number
        sequence: NonZero<u64>,
        /// Pre-serialized entries (not serialized, only for in-memory passing)
        #[serde(skip)]
        entries: Option<Arc<Vec<bytes::Bytes>>>,
    },
    /// Stream trimmed
    Trimmed {
        /// Stream name
        stream: StreamName,
        /// New start sequence
        new_start_seq: NonZero<u64>,
    },
    /// Message deleted
    Deleted {
        /// Stream name
        stream: StreamName,
        /// Deleted sequence number
        sequence: NonZero<u64>,
    },
    /// Error response
    Error {
        /// Error message
        message: String,
    },
}

impl GroupResponse {
    /// Create an error response
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }

    /// Create a success response
    pub fn success() -> Self {
        Self::Success
    }
}

/// Stored message with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    /// Sequence number
    pub sequence: u64,
    /// Message data
    pub data: MessageData,
    /// Timestamp
    pub timestamp: u64,
}
