//! Message types for VSOCK logging protocol

use bytes::Bytes;
use proven_vsock_rpc::{RpcMessage, error::CodecError};
use serde::{Deserialize, Serialize};
use tracing::Level;

/// A single log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log level
    pub level: LogLevel,
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
    /// Component/span name
    pub component: Option<String>,
    /// Span ID
    pub span_id: Option<u64>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: u64,
}

/// Serializable log level
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogLevel {
    /// Error level
    Error,
    /// Warn level
    Warn,
    /// Info level
    Info,
    /// Debug level
    Debug,
    /// Trace level
    Trace,
}

impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::ERROR => LogLevel::Error,
            Level::WARN => LogLevel::Warn,
            Level::INFO => LogLevel::Info,
            Level::DEBUG => LogLevel::Debug,
            Level::TRACE => LogLevel::Trace,
        }
    }
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => Level::ERROR,
            LogLevel::Warn => Level::WARN,
            LogLevel::Info => Level::INFO,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Trace => Level::TRACE,
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
