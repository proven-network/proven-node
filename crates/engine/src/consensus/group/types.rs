//! Types for group consensus layer

use serde::{Deserialize, Serialize};

pub use crate::services::stream::StreamMessage as MessageData;
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
    /// Append message to stream
    Append {
        /// Stream name
        stream: StreamName,
        /// Message data
        message: MessageData,
    },
    /// Trim stream
    Trim {
        /// Stream name
        stream: StreamName,
        /// Trim up to this sequence
        up_to_seq: u64,
    },
    /// Delete a specific message from stream
    Delete {
        /// Stream name
        stream: StreamName,
        /// Sequence number to delete
        sequence: u64,
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
pub enum GroupResponse {
    /// Operation succeeded
    Success,
    /// Message appended
    Appended {
        /// Stream name
        stream: StreamName,
        /// Assigned sequence number
        sequence: u64,
    },
    /// Stream trimmed
    Trimmed {
        /// Stream name
        stream: StreamName,
        /// New start sequence
        new_start_seq: u64,
    },
    /// Message deleted
    Deleted {
        /// Stream name
        stream: StreamName,
        /// Deleted sequence number
        sequence: u64,
    },
    /// Error response
    Error {
        /// Error message
        message: String,
    },
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
