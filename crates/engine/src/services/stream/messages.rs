//! Messages for stream service

use proven_network::{NetworkMessage, ServiceMessage};
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};

use crate::foundation::{StreamConfig, StreamName};
use crate::services::stream::{StreamInfo, StreamMessage};

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
    CreateStream {
        stream_name: StreamName,
        config: StreamConfig,
    },
    /// Delete a stream
    DeleteStream { stream_name: StreamName },
    /// Get stream information
    GetStreamInfo { stream_name: StreamName },
    /// Read from a stream
    ReadStream {
        stream_name: StreamName,
        sequence_range: StreamSequenceRange,
    },
    /// Query stream (streaming read) - unimplemented for now
    QueryStream {
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
    /// Stream information
    StreamInfo(Option<StreamInfo>),
    /// Messages read from stream
    Messages(Vec<StreamMessage>),
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
