//! Operation handler infrastructure for processing consensus operations
//!
//! This module provides a trait-based system for handling different operation types,
//! replacing the old state_command pattern with a more modular and extensible approach.

use async_trait::async_trait;

use crate::{core::global::GlobalState, error::ConsensusResult};
use proven_topology::NodeId;

pub mod group_handler;
pub mod group_responses;
pub mod group_stream_handler;
pub mod node_handler;
pub mod registry;
pub mod responses;
pub mod routing_handler;
pub mod stream_handler;

// Re-export main types
pub use group_handler::GroupOperationHandler;
pub use group_responses::*;
pub use group_stream_handler::{
    GroupOperationContext, GroupStreamHandlerRegistry, GroupStreamOperationHandler,
};
pub use node_handler::NodeOperationHandler;
pub use registry::OperationHandlerRegistry;
pub use responses::*;
pub use routing_handler::RoutingOperationHandler;
pub use stream_handler::StreamManagementHandler;

/// Context for executing operations
#[derive(Debug, Clone)]
pub struct OperationContext {
    /// The sequence number of this operation in the log
    pub sequence: u64,
    /// The timestamp when this operation was created
    pub timestamp: u64,
    /// The node that proposed this operation
    pub proposer_node_id: NodeId,
    /// Whether this node is currently the leader
    pub is_leader: bool,
}

impl OperationContext {
    /// Create a new operation context
    pub fn new(sequence: u64, proposer_node_id: NodeId, is_leader: bool) -> Self {
        Self {
            sequence,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            proposer_node_id,
            is_leader,
        }
    }
}

/// Trait for handling global operations
#[async_trait]
pub trait GlobalOperationHandler: Send + Sync {
    /// The specific operation type this handler processes
    type Operation: Send + Sync;
    /// The specific response type this handler returns
    type Response: Into<GlobalOperationResponse> + Send + Sync;

    /// Apply the operation to the global state
    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<Self::Response>;

    /// Pre-execution hook for setup or additional validation
    async fn pre_execute(
        &self,
        _operation: &Self::Operation,
        _state: &GlobalState,
        _context: &OperationContext,
    ) -> ConsensusResult<()> {
        Ok(())
    }

    /// Post-execution hook for cleanup, metrics, or event emission
    async fn post_execute(
        &self,
        _operation: &Self::Operation,
        _response: &Self::Response,
        _state: &GlobalState,
        _context: &OperationContext,
    ) -> ConsensusResult<()> {
        Ok(())
    }

    /// Get the operation type name for logging and metrics
    fn operation_type(&self) -> &'static str;
}

/// Extension trait for executing operations with hooks
#[async_trait]
pub trait HandlerExecutor: GlobalOperationHandler {
    /// Execute the operation with pre/post hooks
    async fn execute(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<Self::Response> {
        // Pre-execution hook
        self.pre_execute(operation, state, context).await?;

        // Apply the operation
        let response = self.apply(operation, state, context).await?;

        // Post-execution hook
        self.post_execute(operation, &response, state, context)
            .await?;

        Ok(response)
    }

    /// Execute and convert to OperationResponse
    async fn execute_as_operation_response(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let response = self.execute(operation, state, context).await?;
        Ok(response.into())
    }
}

// Implement HandlerExecutor for all GlobalOperationHandler types
impl<T: GlobalOperationHandler> HandlerExecutor for T {}

/// Result of handler execution
#[derive(Debug)]
pub struct HandlerResult {
    /// The response from the operation
    pub response: GlobalOperationResponse,
    /// Any events that should be emitted
    pub events: Vec<HandlerEvent>,
}

/// Events that can be emitted by handlers
#[derive(Debug, Clone)]
pub enum HandlerEvent {
    /// A stream was created
    StreamCreated {
        /// The name of the stream
        name: String,
        /// The group ID of the stream
        group_id: crate::ConsensusGroupId,
    },
    /// A stream was deleted
    StreamDeleted {
        /// The name of the stream
        name: String,
    },
    /// A group was created
    GroupCreated {
        /// The group ID of the group
        group_id: crate::ConsensusGroupId,
        /// The members of the group
        members: Vec<NodeId>,
    },
    /// A group was deleted
    GroupDeleted {
        /// The group ID of the group
        group_id: crate::ConsensusGroupId,
    },
    /// A node joined a group
    NodeJoinedGroup {
        /// The node ID of the node
        node_id: NodeId,
        /// The group ID of the group
        group_id: crate::ConsensusGroupId,
    },
    /// A node left a group
    NodeLeftGroup {
        /// The node ID of the node
        node_id: NodeId,
        /// The group ID of the group
        group_id: crate::ConsensusGroupId,
    },
    /// A routing subscription was added
    RoutingSubscriptionAdded {
        /// The name of the stream
        stream_name: String,
        /// The subject pattern of the subscription
        subject_pattern: String,
    },
    /// A routing subscription was removed
    RoutingSubscriptionRemoved {
        /// The name of the stream
        stream_name: String,
        /// The subject pattern of the subscription
        subject_pattern: String,
    },
}
