//! Error types for the consensus system

use std::fmt;
use thiserror::Error;
use tokio::task::JoinError;

/// Result type for consensus operations
pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// Main error type for consensus system
#[derive(Debug, Error)]
pub struct ConsensusError {
    /// Error kind
    kind: ErrorKind,
    /// Error context
    context: ErrorContext,
}

impl ConsensusError {
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
}

impl fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.context {
            ErrorContext::Message(msg) => write!(f, "{}: {}", self.kind, msg),
            ErrorContext::Chain { message, source } => {
                write!(f, "{}: {} (caused by: {})", self.kind, message, source)
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
}

// Conversion implementations for common error types

impl From<std::io::Error> for ConsensusError {
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

impl From<serde_json::Error> for ConsensusError {
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

impl From<tokio::task::JoinError> for ConsensusError {
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
impl From<crate::services::cluster::ClusterError> for ConsensusError {
    fn from(err: crate::services::cluster::ClusterError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Cluster error: {err}"))
    }
}

impl From<crate::services::event::EventError> for ConsensusError {
    fn from(err: crate::services::event::EventError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Event error: {err}"))
    }
}

impl From<crate::services::monitoring::MonitoringError> for ConsensusError {
    fn from(err: crate::services::monitoring::MonitoringError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Monitoring error: {err}"))
    }
}

impl From<crate::services::routing::RoutingError> for ConsensusError {
    fn from(err: crate::services::routing::RoutingError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Routing error: {err}"))
    }
}

impl From<crate::services::migration::MigrationError> for ConsensusError {
    fn from(err: crate::services::migration::MigrationError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Migration error: {err}"))
    }
}

impl From<crate::services::lifecycle::LifecycleError> for ConsensusError {
    fn from(err: crate::services::lifecycle::LifecycleError) -> Self {
        Self::with_context(ErrorKind::Service, format!("Lifecycle error: {err}"))
    }
}

impl From<crate::services::pubsub::PubSubError> for ConsensusError {
    fn from(err: crate::services::pubsub::PubSubError) -> Self {
        Self::with_context(ErrorKind::Service, format!("PubSub error: {err}"))
    }
}

impl From<crate::services::network::NetworkError> for ConsensusError {
    fn from(err: crate::services::network::NetworkError) -> Self {
        Self::with_context(ErrorKind::Network, format!("Network service error: {err}"))
    }
}

impl From<proven_network::NetworkError> for ConsensusError {
    fn from(err: proven_network::NetworkError) -> Self {
        Self::with_context(ErrorKind::Network, format!("Network error: {err}"))
    }
}

/// Helper macro for creating errors with context
#[macro_export]
macro_rules! consensus_error {
    ($kind:expr, $($arg:tt)*) => {
        $crate::foundation::ConsensusError::with_context($kind, format!($($arg)*))
    };
}
