//! Message types for communication between TUI and async components

use chrono::{DateTime, Utc};
use proven_governance_mock::MockGovernance;
use proven_local::NodeConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

/// Type alias for the `NodeConfig` we use in the TUI
pub type TuiNodeConfig = NodeConfig<MockGovernance>;

// Re-export NodeId and related constants/functions from the node_id module
pub use crate::node_id::{MAIN_THREAD_NODE_ID, NodeId};

/// Per-node operations processed by individual node threads
#[derive(Debug, Clone)]
pub enum NodeOperation {
    /// Start the node
    Start {
        /// Node configuration (optional - will use existing config if None)
        config: Option<Box<TuiNodeConfig>>,
        /// Specializations for this node (used for governance registration and symlinks)
        specializations: Option<HashSet<proven_governance::NodeSpecialization>>,
    },
    /// Stop the node
    Stop,
    /// Restart the node (stop then start)
    Restart,
    /// Shutdown the node permanently
    Shutdown,
}

/// Log level for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    /// Debug level
    Debug,

    /// Error level
    Error,

    /// Info level
    Info,

    /// Trace level
    Trace,

    /// Warning level
    Warn,
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
