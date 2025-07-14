//! Type-safe operation definitions and traits
//!
//! This module provides a strongly-typed operation system with
//! proper validation and extensibility.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::ConsensusGroupId;
use crate::error::Error as ConsensusError;
use crate::operations::{GlobalOperation, GroupStreamOperation};
use proven_topology::NodeId;

/// Base trait for all consensus operations
#[async_trait]
pub trait Operation: Send + Sync + Debug + Serialize + for<'de> Deserialize<'de> {
    /// The response type for this operation
    type Response: Send + Sync + Debug;

    /// Validate the operation before execution
    fn validate(&self) -> Result<(), OperationError>;

    /// Get the operation type name for logging/metrics
    fn type_name(&self) -> &'static str;

    /// Check if this operation is read-only
    fn is_readonly(&self) -> bool {
        false
    }

    /// Get the priority of this operation
    fn priority(&self) -> OperationPriority {
        OperationPriority::Normal
    }
}

/// Unified operation type that can be either global or local
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusOperation {
    /// Global consensus operation
    Global(GlobalOp),
    /// Local consensus operation
    Local(LocalOp),
}

impl ConsensusOperation {
    /// Check if this is a global operation
    pub fn is_global(&self) -> bool {
        matches!(self, ConsensusOperation::Global(_))
    }

    /// Check if this is a local operation
    pub fn is_local(&self) -> bool {
        matches!(self, ConsensusOperation::Local(_))
    }

    /// Get the operation context
    pub fn context(&self) -> &OperationContext {
        match self {
            ConsensusOperation::Global(op) => &op.context,
            ConsensusOperation::Local(op) => &op.context,
        }
    }
}

/// Global operation wrapper with context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalOp {
    /// The actual operation
    pub operation: GlobalOperation,
    /// Operation context
    pub context: OperationContext,
}

impl GlobalOp {
    /// Create a new global operation
    pub fn new(operation: GlobalOperation) -> Self {
        Self {
            operation,
            context: OperationContext::default(),
        }
    }

    /// Create with custom context
    pub fn with_context(operation: GlobalOperation, context: OperationContext) -> Self {
        Self { operation, context }
    }
}

/// Local operation wrapper with routing info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalOp {
    /// Target consensus group
    pub group_id: ConsensusGroupId,
    /// The actual operation
    pub operation: GroupStreamOperation,
    /// Operation context
    pub context: OperationContext,
}

impl LocalOp {
    /// Create a new local operation
    pub fn new(group_id: ConsensusGroupId, operation: GroupStreamOperation) -> Self {
        Self {
            group_id,
            operation,
            context: OperationContext::default(),
        }
    }

    /// Create with custom context
    pub fn with_context(
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
        context: OperationContext,
    ) -> Self {
        Self {
            group_id,
            operation,
            context,
        }
    }
}

/// Context information for operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationContext {
    /// Node that initiated the operation
    pub initiator: Option<NodeId>,
    /// Timestamp when operation was created
    pub timestamp: u64,
    /// Operation priority
    pub priority: OperationPriority,
    /// Tracing/correlation ID
    pub trace_id: Option<String>,
    /// Whether to wait for quorum confirmation
    pub require_quorum: bool,
}

impl Default for OperationContext {
    fn default() -> Self {
        Self {
            initiator: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            priority: OperationPriority::Normal,
            trace_id: None,
            require_quorum: true,
        }
    }
}

/// Operation priority levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum OperationPriority {
    /// Low priority operations
    Low = 0,
    /// Normal priority (default)
    Normal = 1,
    /// High priority operations
    High = 2,
    /// System-critical operations
    Critical = 3,
}

/// Errors specific to operations
#[derive(Debug, thiserror::Error)]
pub enum OperationError {
    /// Operation validation failed
    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Invalid operation state
    #[error("Invalid operation state: {0}")]
    InvalidState(String),

    /// Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Resource not found
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Operation timeout
    #[error("Operation timed out")]
    Timeout,

    /// Generic consensus error
    #[error("Consensus error: {0}")]
    Consensus(#[from] ConsensusError),
}

/// Builder for operations with fluent API
pub struct OperationBuilder<O> {
    operation: O,
    context: OperationContext,
}

impl<O> OperationBuilder<O> {
    /// Create a new builder
    pub fn new(operation: O) -> Self {
        Self {
            operation,
            context: OperationContext::default(),
        }
    }

    /// Set the initiator
    pub fn initiator(mut self, node_id: NodeId) -> Self {
        self.context.initiator = Some(node_id);
        self
    }

    /// Set the priority
    pub fn priority(mut self, priority: OperationPriority) -> Self {
        self.context.priority = priority;
        self
    }

    /// Set the trace ID
    pub fn trace_id(mut self, trace_id: String) -> Self {
        self.context.trace_id = Some(trace_id);
        self
    }

    /// Set quorum requirement
    pub fn require_quorum(mut self, require: bool) -> Self {
        self.context.require_quorum = require;
        self
    }
}

impl OperationBuilder<GlobalOperation> {
    /// Build a global operation
    pub fn build_global(self) -> ConsensusOperation {
        ConsensusOperation::Global(GlobalOp {
            operation: self.operation,
            context: self.context,
        })
    }
}

impl OperationBuilder<GroupStreamOperation> {
    /// Build a local operation
    pub fn build_local(self, group_id: ConsensusGroupId) -> ConsensusOperation {
        ConsensusOperation::Local(LocalOp {
            group_id,
            operation: self.operation,
            context: self.context,
        })
    }
}

/// Helper to create operation builders
pub fn operation<O>(op: O) -> OperationBuilder<O> {
    OperationBuilder::new(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::StreamManagementOperation;

    #[test]
    fn test_operation_builder() {
        let op = operation(GlobalOperation::StreamManagement(
            StreamManagementOperation::Create {
                name: "test".to_string(),
                config: Default::default(),
                group_id: ConsensusGroupId::new(1),
            },
        ))
        .priority(OperationPriority::High)
        .trace_id("test-trace".to_string())
        .build_global();

        assert!(op.is_global());
        assert_eq!(op.context().priority, OperationPriority::High);
        assert_eq!(op.context().trace_id.as_deref(), Some("test-trace"));
    }
}
