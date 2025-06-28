//! Message types for communication between TUI and async components

use chrono::{DateTime, Utc};
use proven_governance_mock::MockGovernance;
use proven_local::NodeConfig;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Type alias for the `NodeConfig` we use in the TUI
pub type TuiNodeConfig = NodeConfig<MockGovernance>;

// Re-export NodeId and related constants/functions from the node_id module
pub use crate::node_id::{MAIN_THREAD_NODE_ID, NodeId};

/// Status of a node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node is starting up
    Starting,
    /// Node is running normally
    Running,
    /// Node is shutting down
    Stopping,
    /// Node has stopped
    Stopped,
    /// Node failed with an error
    Failed(String),
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Stopping => write!(f, "Stopping"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Failed(err) => write!(f, "Failed: {err}"),
        }
    }
}

/// Log level for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    /// Error level
    Error,
    /// Warning level
    Warn,
    /// Info level
    Info,
    /// Debug level
    Debug,
    /// Trace level
    Trace,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => write!(f, "ERROR"),
            Self::Warn => write!(f, "WARN"),
            Self::Info => write!(f, "INFO"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Trace => write!(f, "TRACE"),
        }
    }
}

/// A log entry from a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The node that generated this log
    pub node_id: NodeId,
    /// Log level
    pub level: LogLevel,
    /// Log message
    pub message: String,
    /// When this log was generated
    pub timestamp: DateTime<Utc>,
    /// Module/component that generated the log
    pub target: Option<String>,
}

/// Messages sent from async tasks to the TUI
#[derive(Debug, Clone)]
pub enum TuiMessage {
    /// A node has started
    NodeStarted {
        /// Node identifier
        id: NodeId,
        /// Node name for display
        name: String,
        /// Node configuration
        config: Box<TuiNodeConfig>,
    },

    /// A node has stopped
    NodeStopped {
        /// Node identifier
        id: NodeId,
    },

    /// A node failed to start or crashed
    NodeFailed {
        /// Node identifier
        id: NodeId,
        /// Error message
        error: String,
    },

    /// Node status update
    NodeStatusUpdate {
        /// Node identifier
        id: NodeId,
        /// New status
        status: NodeStatus,
    },

    /// A log entry from a node
    LogEntry(LogEntry),

    /// System metrics update (for future use)
    SystemMetrics {
        /// CPU usage percentage
        cpu_usage: f32,
        /// Memory usage in bytes
        memory_usage: u64,
        /// Available memory in bytes
        memory_total: u64,
    },

    /// All nodes have been shut down successfully
    ShutdownComplete,
}

/// Commands sent from the TUI to async tasks
#[derive(Debug, Clone)]
pub enum NodeCommand {
    /// Start a new node
    StartNode {
        /// Node identifier
        id: NodeId,
        /// Display name for the node
        name: String,
        /// Node configuration (optional - `NodeManager` will create if None)
        config: Option<Box<TuiNodeConfig>>,
    },

    /// Stop a running node
    StopNode {
        /// Node identifier
        id: NodeId,
    },

    /// Restart a node (stop then start)
    RestartNode {
        /// Node identifier
        id: NodeId,
    },

    /// Get status of all nodes
    GetStatus,

    /// Shutdown the entire TUI
    Shutdown,
}
