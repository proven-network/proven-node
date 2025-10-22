//! Types for group consensus layer

use std::fmt;
use std::sync::Arc;

use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::foundation::{Message, StreamName};

/// Group consensus request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupRequest {
    /// Stream operation
    Stream(StreamOperation),
    /// Administrative operation
    Admin(AdminOperation),
}

impl fmt::Display for GroupRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(op) => write!(f, "Stream({})", op),
            Self::Admin(op) => write!(f, "Admin({})", op),
        }
    }
}

/// Stream operation within a group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOperation {
    /// Append messages to stream
    Append {
        /// Stream name
        stream_name: StreamName,
        /// Messages to append
        messages: Vec<Message>,
        /// Timestamp assigned by the leader (milliseconds since epoch)
        timestamp: u64,
    },
    /// Trim stream
    Trim {
        /// Stream name
        stream_name: StreamName,
        /// Trim up to this sequence
        up_to_seq: LogIndex,
    },
    /// Delete a specific message from stream
    Delete {
        /// Stream name
        stream_name: StreamName,
        /// Sequence number to delete
        sequence: LogIndex,
    },
}

impl fmt::Display for StreamOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Append {
                stream_name,
                messages,
                ..
            } => {
                write!(f, "Append({}, {} messages)", stream_name, messages.len())
            }
            Self::Trim {
                stream_name,
                up_to_seq,
            } => {
                write!(f, "Trim({}, up_to: {})", stream_name, up_to_seq)
            }
            Self::Delete {
                stream_name,
                sequence,
            } => {
                write!(f, "Delete({}, seq: {})", stream_name, sequence)
            }
        }
    }
}

/// Administrative operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminOperation {
    /// Initialize stream in this group
    InitializeStream {
        /// Stream name
        stream_name: StreamName,
    },
    /// Remove stream from this group
    RemoveStream {
        /// Stream name
        stream_name: StreamName,
    },
    /// Compact storage
    Compact,
}

impl fmt::Display for AdminOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InitializeStream { stream_name } => {
                write!(f, "InitializeStream({})", stream_name)
            }
            Self::RemoveStream { stream_name } => {
                write!(f, "RemoveStream({})", stream_name)
            }
            Self::Compact => write!(f, "Compact"),
        }
    }
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
        stream_name: StreamName,
        /// Assigned sequence number
        sequence: LogIndex,
        /// Pre-serialized entries (not serialized, only for in-memory passing)
        #[serde(skip)]
        entries: Option<Arc<Vec<bytes::Bytes>>>,
    },
    /// Stream trimmed
    Trimmed {
        /// Stream name
        stream_name: StreamName,
        /// New start sequence
        new_start_seq: LogIndex,
    },
    /// Message deleted
    Deleted {
        /// Stream name
        stream_name: StreamName,
        /// Deleted sequence number
        sequence: LogIndex,
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
