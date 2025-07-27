//! Message types and traits for the network layer

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Base trait for all network messages
pub trait NetworkMessage: Send + Sync + 'static {
    /// Get the message type identifier
    fn message_type() -> &'static str
    where
        Self: Sized;

    /// Serialize the message
    fn serialize(&self) -> Result<Bytes, crate::error::NetworkError>
    where
        Self: Sized + Serialize,
    {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf)
            .map_err(|e| crate::error::NetworkError::Serialization(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    /// Deserialize a message
    fn deserialize(bytes: &[u8]) -> Result<Self, crate::error::NetworkError>
    where
        Self: Sized + for<'de> Deserialize<'de>,
    {
        ciborium::from_reader(bytes)
            .map_err(|e| crate::error::NetworkError::Serialization(e.to_string()))
    }
}

/// Trait for service messages (request-response pattern)
pub trait ServiceMessage: NetworkMessage + Serialize + for<'de> Deserialize<'de> {
    /// Response type for this message
    type Response: NetworkMessage + Serialize + for<'de> Deserialize<'de>;

    /// Service identifier
    fn service_id() -> &'static str;

    /// Default timeout for requests
    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Trait for streaming service messages
pub trait StreamingServiceMessage: ServiceMessage {
    /// The type of items in the stream
    type Item: NetworkMessage + Serialize + for<'de> Deserialize<'de>;

    /// Stream type identifier
    fn stream_type() -> &'static str;
}

/// Multiplexed frame for connection-level routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiplexedFrame {
    /// Stream ID (None for connection-level messages)
    pub stream_id: Option<uuid::Uuid>,
    /// Frame data
    pub data: FrameData,
}

/// Types of frame data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameData {
    /// Stream frame
    Stream(crate::stream::StreamFrame),
    /// Connection control
    Control(ControlFrame),
}

/// Connection-level control frames
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlFrame {
    /// Ping for keep-alive
    Ping { data: Bytes },
    /// Pong response
    Pong { data: Bytes },
    /// Connection close
    Close { code: u32, reason: String },
}

impl MultiplexedFrame {
    /// Serialize a frame to bytes
    pub fn serialize(&self) -> Result<Bytes, crate::error::NetworkError> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf)
            .map_err(|e| crate::error::NetworkError::Serialization(e.to_string()))?;
        Ok(Bytes::from(buf))
    }

    /// Deserialize a frame from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self, crate::error::NetworkError> {
        ciborium::from_reader(bytes)
            .map_err(|e| crate::error::NetworkError::Serialization(e.to_string()))
    }
}
