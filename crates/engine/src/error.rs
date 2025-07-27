//! Error types for the consensus system

use std::fmt;

use proven_topology::NodeId;
use thiserror::Error;

/// Result type for consensus operations
pub type ConsensusResult<T> = Result<T, Error>;

/// Main error type for consensus system
#[derive(Debug, Error)]
pub struct Error {
    /// Error kind
    kind: ErrorKind,
    /// Error context
    context: ErrorContext,
}

impl Error {
    /// Create a new error
    pub fn new(kind: ErrorKind, context: ErrorContext) -> Self {
        Self { kind, context }
    }

    /// Create error with string context
    pub fn with_context(kind: ErrorKind, context: impl Into<String>) -> Self {
        Self {
            kind,
            context: ErrorContext::Message(context.into()),
        }
    }

    /// Get error kind
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Get error context
    pub fn context(&self) -> &ErrorContext {
        &self.context
    }

    /// Create a not found error
    pub fn not_found(what: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::NotFound, what)
    }

    /// Create an invalid state error
    pub fn invalid_state(msg: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::InvalidState, msg)
    }

    /// Create an operation failed error
    pub fn operation_failed(msg: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::OperationFailed, msg)
    }

    /// Create a timeout error
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::Timeout, msg)
    }

    /// Create a network error
    pub fn network(msg: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::Network, msg)
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::with_context(ErrorKind::Storage, msg)
    }

    /// Create a not leader error
    pub fn not_leader(msg: impl Into<String>, current_leader: Option<NodeId>) -> Self {
        Self {
            kind: ErrorKind::NotLeader,
            context: ErrorContext::Leadership {
                message: msg.into(),
                current_leader,
            },
        }
    }

    /// Check if this is a not-leader error
    pub fn is_not_leader(&self) -> bool {
        matches!(self.kind, ErrorKind::NotLeader)
    }

    /// Get the current leader from a not-leader error
    pub fn get_leader(&self) -> Option<&NodeId> {
        match &self.context {
            ErrorContext::Leadership { current_leader, .. } => current_leader.as_ref(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.context {
            ErrorContext::Message(msg) => write!(f, "{}: {}", self.kind, msg),
            ErrorContext::Chain { message, source } => {
                write!(f, "{}: {} (caused by: {})", self.kind, message, source)
            }
            ErrorContext::Leadership {
                message,
                current_leader,
            } => {
                if let Some(leader) = current_leader {
                    write!(f, "{}: {} (current leader: {})", self.kind, message, leader)
                } else {
                    write!(f, "{}: {} (no known leader)", self.kind, message)
                }
            }
        }
    }
}

/// Error kinds
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Resource not found
    NotFound,
    /// Invalid state for operation
    InvalidState,
    /// Operation failed
    OperationFailed,
    /// Operation timed out
    Timeout,
    /// Network error
    Network,
    /// Storage error
    Storage,
    /// Configuration error
    Configuration,
    /// Consensus error (raft)
    Consensus,
    /// Service error
    Service,
    /// Validation error
    Validation,
    /// Internal error
    Internal,
    /// Cluster formation error
    ClusterFormation,
    /// Not the leader
    NotLeader,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::NotFound => write!(f, "Not found"),
            ErrorKind::InvalidState => write!(f, "Invalid state"),
            ErrorKind::OperationFailed => write!(f, "Operation failed"),
            ErrorKind::Timeout => write!(f, "Timeout"),
            ErrorKind::Network => write!(f, "Network error"),
            ErrorKind::Storage => write!(f, "Storage error"),
            ErrorKind::Configuration => write!(f, "Configuration error"),
            ErrorKind::Consensus => write!(f, "Consensus error"),
            ErrorKind::Service => write!(f, "Service error"),
            ErrorKind::Validation => write!(f, "Validation error"),
            ErrorKind::Internal => write!(f, "Internal error"),
            ErrorKind::ClusterFormation => write!(f, "Cluster formation error"),
            ErrorKind::NotLeader => write!(f, "Not the leader"),
        }
    }
}

/// Error context
#[derive(Debug)]
pub enum ErrorContext {
    /// Simple message
    Message(String),
    /// Error chain with source
    Chain {
        /// Error message
        message: String,
        /// Source error
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Leadership error with info
    Leadership {
        /// Error message
        message: String,
        /// Current leader if known
        current_leader: Option<NodeId>,
    },
}

// Conversion implementations for common error types

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {
            kind: ErrorKind::Storage,
            context: ErrorContext::Chain {
                message: "I/O error".to_string(),
                source: Box::new(err),
            },
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::Internal,
            context: ErrorContext::Chain {
                message: "Serialization error".to_string(),
                source: Box::new(err),
            },
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Self {
            kind: ErrorKind::Internal,
            context: ErrorContext::Chain {
                message: "Task join error".to_string(),
                source: Box::new(err),
            },
        }
    }
}

// Allow services to convert their errors to consensus errors
impl From<crate::services::monitoring::MonitoringError> for Error {
    fn from(err: crate::services::monitoring::MonitoringError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Monitoring error: {err}"))
    }
}

impl From<crate::foundation::routing::RoutingError> for Error {
    fn from(err: crate::foundation::routing::RoutingError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Routing error: {err}"))
    }
}

impl From<crate::services::migration::MigrationError> for Error {
    fn from(err: crate::services::migration::MigrationError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Migration error: {err}"))
    }
}

impl From<crate::services::lifecycle::LifecycleError> for Error {
    fn from(err: crate::services::lifecycle::LifecycleError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Lifecycle error: {err}"))
    }
}

impl From<crate::services::pubsub::PubSubError> for Error {
    fn from(err: crate::services::pubsub::PubSubError) -> Self {
        Self::with_context(ErrorKind::Service, format!("PubSub error: {err}"))
    }
}

impl From<proven_network::NetworkError> for Error {
    fn from(err: proven_network::NetworkError) -> Self {
        Self::with_context(ErrorKind::Network, format!("Network error: {err}"))
    }
}

impl From<crate::foundation::events::Error> for Error {
    fn from(err: crate::foundation::events::Error) -> Self {
        use crate::foundation::events::Error as EventError;

        match err {
            EventError::NoHandler { type_name } => Self::with_context(
                ErrorKind::NotFound,
                format!("No handler registered for {type_name}"),
            ),
            EventError::Timeout { duration } => Self::with_context(
                ErrorKind::Timeout,
                format!("Request timed out after {duration:?}"),
            ),
            EventError::Internal(msg) => {
                // Check if this is a not-leader error passed through internal
                if msg.contains("not the leader") || msg.contains("NotLeader") {
                    Self::not_leader("Not the leader for this operation", None)
                } else {
                    Self::with_context(ErrorKind::Internal, format!("Internal error: {msg}"))
                }
            }
            EventError::ChannelFull => {
                Self::with_context(ErrorKind::InvalidState, "Event channel is full")
            }
            EventError::HandlerPanic => Self::with_context(
                ErrorKind::Internal,
                "Handler panicked while processing event",
            ),
            EventError::Shutdown => {
                Self::with_context(ErrorKind::InvalidState, "Event bus is shutting down")
            }
            EventError::InvalidConfig { reason } => Self::with_context(
                ErrorKind::Configuration,
                format!("Invalid configuration: {reason}"),
            ),
            EventError::DeadlineExceeded => {
                Self::with_context(ErrorKind::Timeout, "Deadline exceeded")
            }
            EventError::StreamEnded => {
                Self::with_context(ErrorKind::InvalidState, "Stream ended unexpectedly")
            }
            EventError::Channel(_) => Self::with_context(ErrorKind::Internal, "Channel send error"),
            EventError::Recv(_) => Self::with_context(ErrorKind::Internal, "Channel receive error"),
            EventError::TryRecv(_) => {
                Self::with_context(ErrorKind::Internal, "Channel try_recv error")
            }
            EventError::Oneshot(_) => {
                Self::with_context(ErrorKind::Internal, "Oneshot channel error")
            }
        }
    }
}

/// Helper macro for creating errors with context
#[macro_export]
macro_rules! consensus_error {
    ($kind:expr, $($arg:tt)*) => {
        $crate::foundation::ConsensusError::with_context($kind, format!($($arg)*))
    };
}
