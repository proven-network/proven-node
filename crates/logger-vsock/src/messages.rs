//! Message types for VSOCK logging protocol

use bytes::Bytes;
use proven_logger::{Level, Record};
use proven_vsock_rpc::{RpcMessage, error::CodecError};
use serde::{Deserialize, Serialize};

/// A single log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log level
    pub level: Level,
    /// Target module
    pub target: String,
    /// Log message
    pub message: String,
    /// Source file
    pub file: Option<String>,
    /// Line number
    pub line: Option<u32>,
    /// Node ID if available
    pub node_id: Option<String>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
}

impl From<Record<'_>> for LogEntry {
    fn from(record: Record) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            level: record.level,
            target: record.target.to_string(),
            message: record.message.to_string(),
            file: record.file.map(|s| s.to_string()),
            line: record.line,
            node_id: record
                .context
                .and_then(|ctx| ctx.node_id.as_ref())
                .map(|id| id.to_string()),
            timestamp,
        }
    }
}

/// A batch of log entries for efficient transmission
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogBatch {
    /// The log entries in this batch
    pub entries: Vec<LogEntry>,
    /// Sequence number for tracking
    pub sequence: u64,
    /// Source enclave ID
    pub enclave_id: Option<String>,
}

impl RpcMessage for LogBatch {
    type Response = LogBatchAck;

    fn message_id(&self) -> &'static str {
        "log_batch"
    }
}

/// Acknowledgment for a log batch (optional, for reliability)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogBatchAck {
    /// Sequence number being acknowledged
    pub sequence: u64,
    /// Number of entries received
    pub entries_received: usize,
}

// Implement conversions for RPC

impl TryFrom<LogBatch> for Bytes {
    type Error = CodecError;

    fn try_from(value: LogBatch) -> Result<Self, Self::Error> {
        bincode::serialize(&value)
            .map(Bytes::from)
            .map_err(|e| CodecError::SerializationFailed(e.to_string()))
    }
}

impl TryFrom<Bytes> for LogBatch {
    type Error = CodecError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value).map_err(|e| CodecError::DeserializationFailed(e.to_string()))
    }
}

impl TryFrom<LogBatchAck> for Bytes {
    type Error = CodecError;

    fn try_from(value: LogBatchAck) -> Result<Self, Self::Error> {
        bincode::serialize(&value)
            .map(Bytes::from)
            .map_err(|e| CodecError::SerializationFailed(e.to_string()))
    }
}

impl TryFrom<Bytes> for LogBatchAck {
    type Error = CodecError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value).map_err(|e| CodecError::DeserializationFailed(e.to_string()))
    }
}
