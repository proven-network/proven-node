//! Categorized consensus operations
//!
//! This module provides a hierarchical operation structure with clear categories
//! for different types of consensus operations. Each category has its own
//! validator and handler.

pub mod group_ops;
pub mod local_stream_ops;
pub mod node_ops;
pub mod routing_ops;
pub mod stream_management_ops;
pub mod sync_validators;
pub mod validators;

use crate::error::ConsensusResult;
use serde::{Deserialize, Serialize};

// Re-export operation types
pub use group_ops::GroupOperation;
pub use local_stream_ops::{
    LocalStreamOperation, MaintenanceOperation, MigrationOperation, PubSubOperation,
    StreamOperation,
};
pub use node_ops::NodeOperation;
pub use routing_ops::RoutingOperation;
pub use stream_management_ops::StreamManagementOperation;

// Re-export validators
pub use validators::{
    GroupOperationValidator, LocalOperationValidator, LocalStreamOperationValidator,
    NodeOperationValidator, RoutingOperationValidator, StreamManagementOperationValidator,
};

// Re-export sync validators
pub use sync_validators::{
    SyncGroupValidator, SyncNodeValidator, SyncRoutingValidator, SyncStreamValidator,
};

/// Categorized global operations for the consensus system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GlobalOperation {
    /// Stream management operations (create, update, delete, etc.)
    StreamManagement(StreamManagementOperation),
    /// Consensus group operations
    Group(GroupOperation),
    /// Node management operations
    Node(NodeOperation),
    /// Routing and subscription operations
    Routing(RoutingOperation),
}

impl GlobalOperation {
    /// Get the operation category as a string
    pub fn category(&self) -> &'static str {
        match self {
            Self::StreamManagement(_) => "stream_management",
            Self::Group(_) => "group",
            Self::Node(_) => "node",
            Self::Routing(_) => "routing",
        }
    }

    /// Get a human-readable operation name
    pub fn operation_name(&self) -> String {
        match self {
            Self::StreamManagement(op) => op.operation_name(),
            Self::Group(op) => op.operation_name(),
            Self::Node(op) => op.operation_name(),
            Self::Routing(op) => op.operation_name(),
        }
    }
}

/// Operation context containing state needed for validation
pub struct OperationContext<'a> {
    /// Reference to global state
    pub global_state: &'a crate::global::global_state::GlobalState,
    /// Current node ID
    pub node_id: crate::NodeId,
    /// Whether this node is the leader
    pub is_leader: bool,
}

/// Local operation context containing state needed for validation
pub struct LocalOperationContext<'a> {
    /// Reference to local state
    pub local_state: &'a crate::local::StorageBackedLocalState,
    /// Current node ID
    pub node_id: crate::NodeId,
    /// Consensus group ID
    pub group_id: crate::allocation::ConsensusGroupId,
    /// Whether this node is the leader
    pub is_leader: bool,
}

/// Trait for validating operations
#[async_trait::async_trait]
pub trait OperationValidator<T: Send + Sync> {
    /// Validate the operation in the given context
    async fn validate(&self, operation: &T, context: &OperationContext<'_>) -> ConsensusResult<()>;
}
