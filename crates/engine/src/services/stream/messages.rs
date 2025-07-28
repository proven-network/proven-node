//! Messages for stream service

use proven_network::{NetworkMessage, ServiceMessage};
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::foundation::{Message, StreamConfig, StreamName};

/// Range of sequence numbers to read from a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSequenceRange {
    /// Start sequence (inclusive)
    pub start: Option<LogIndex>,
    /// End sequence (exclusive)
    pub end: Option<LogIndex>,
}

/// Stream service message for network communication
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum StreamServiceMessage {
    /// Create a new stream
    Create {
        stream_name: StreamName,
        config: StreamConfig,
    },
    /// Delete a stream
    Delete { stream_name: StreamName },
    /// Read from a stream
    Read {
        stream_name: StreamName,
        sequence_range: StreamSequenceRange,
    },
    /// Query stream (streaming read) - unimplemented for now
    Query {
        stream_name: StreamName,
        sequence_range: StreamSequenceRange,
    },
}

/// Stream service response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum StreamServiceResponse {
    /// Stream created successfully
    StreamCreated,
    /// Stream deleted successfully
    StreamDeleted,
    /// Messages read from stream
    Messages(Vec<Message>),
    /// Error response
    Error(String),
}

impl NetworkMessage for StreamServiceMessage {
    fn message_type() -> &'static str {
        "stream_message"
    }
}

impl NetworkMessage for StreamServiceResponse {
    fn message_type() -> &'static str {
        "stream_response"
    }
}

impl ServiceMessage for StreamServiceMessage {
    type Response = StreamServiceResponse;

    fn service_id() -> &'static str {
        "stream"
    }
}
