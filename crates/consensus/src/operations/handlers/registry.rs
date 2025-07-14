//! Registry for operation handlers

use std::sync::Arc;

use crate::{core::global::GlobalState, error::ConsensusResult, operations::GlobalOperation};

use super::{
    GlobalOperationResponse, GroupOperationHandler, HandlerExecutor, NodeOperationHandler,
    OperationContext, RoutingOperationHandler, StreamManagementHandler,
};

/// Registry that manages all operation handlers
pub struct OperationHandlerRegistry {
    /// Handler for stream management operations
    stream_handler: Arc<StreamManagementHandler>,
    /// Handler for group operations
    group_handler: Arc<GroupOperationHandler>,
    /// Handler for node operations
    node_handler: Arc<NodeOperationHandler>,
    /// Handler for routing operations
    routing_handler: Arc<RoutingOperationHandler>,
}

impl OperationHandlerRegistry {
    /// Create a new operation handler registry
    pub fn new(
        stream_handler: Arc<StreamManagementHandler>,
        group_handler: Arc<GroupOperationHandler>,
        node_handler: Arc<NodeOperationHandler>,
        routing_handler: Arc<RoutingOperationHandler>,
    ) -> Self {
        Self {
            stream_handler,
            group_handler,
            node_handler,
            routing_handler,
        }
    }

    /// Create a registry with default handlers
    pub fn with_defaults(
        event_bus: Option<Arc<crate::core::engine::events::EventBus>>,
        max_groups_per_node: usize,
    ) -> Self {
        Self {
            stream_handler: Arc::new(StreamManagementHandler::new(event_bus.clone())),
            group_handler: Arc::new(GroupOperationHandler::new(event_bus)),
            node_handler: Arc::new(NodeOperationHandler::new(max_groups_per_node)),
            routing_handler: Arc::new(RoutingOperationHandler::new()),
        }
    }

    /// Handle a global operation by delegating to the appropriate handler
    pub async fn handle_operation(
        &self,
        operation: &GlobalOperation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<GlobalOperationResponse> {
        match operation {
            GlobalOperation::StreamManagement(op) => {
                self.stream_handler
                    .execute_as_operation_response(op, state, context)
                    .await
            }
            GlobalOperation::Group(op) => {
                self.group_handler
                    .execute_as_operation_response(op, state, context)
                    .await
            }
            GlobalOperation::Node(op) => {
                self.node_handler
                    .execute_as_operation_response(op, state, context)
                    .await
            }
            GlobalOperation::Routing(op) => {
                self.routing_handler
                    .execute_as_operation_response(op, state, context)
                    .await
            }
        }
    }

    /// Get the stream handler
    pub fn stream_handler(&self) -> &Arc<StreamManagementHandler> {
        &self.stream_handler
    }

    /// Get the group handler
    pub fn group_handler(&self) -> &Arc<GroupOperationHandler> {
        &self.group_handler
    }

    /// Get the node handler
    pub fn node_handler(&self) -> &Arc<NodeOperationHandler> {
        &self.node_handler
    }

    /// Get the routing handler
    pub fn routing_handler(&self) -> &Arc<RoutingOperationHandler> {
        &self.routing_handler
    }
}

/// Builder for creating a customized operation handler registry
pub struct RegistryBuilder {
    stream_handler: Option<Arc<StreamManagementHandler>>,
    group_handler: Option<Arc<GroupOperationHandler>>,
    node_handler: Option<Arc<NodeOperationHandler>>,
    routing_handler: Option<Arc<RoutingOperationHandler>>,
}

impl RegistryBuilder {
    /// Create a new registry builder
    pub fn new() -> Self {
        Self {
            stream_handler: None,
            group_handler: None,
            node_handler: None,
            routing_handler: None,
        }
    }

    /// Set the stream handler
    pub fn with_stream_handler(mut self, handler: Arc<StreamManagementHandler>) -> Self {
        self.stream_handler = Some(handler);
        self
    }

    /// Set the group handler
    pub fn with_group_handler(mut self, handler: Arc<GroupOperationHandler>) -> Self {
        self.group_handler = Some(handler);
        self
    }

    /// Set the node handler
    pub fn with_node_handler(mut self, handler: Arc<NodeOperationHandler>) -> Self {
        self.node_handler = Some(handler);
        self
    }

    /// Set the routing handler
    pub fn with_routing_handler(mut self, handler: Arc<RoutingOperationHandler>) -> Self {
        self.routing_handler = Some(handler);
        self
    }

    /// Build the registry with default handlers for any not explicitly set
    pub fn build(
        self,
        event_bus: Option<Arc<crate::core::engine::events::EventBus>>,
        max_groups_per_node: usize,
    ) -> OperationHandlerRegistry {
        OperationHandlerRegistry {
            stream_handler: self
                .stream_handler
                .unwrap_or_else(|| Arc::new(StreamManagementHandler::new(event_bus.clone()))),
            group_handler: self
                .group_handler
                .unwrap_or_else(|| Arc::new(GroupOperationHandler::new(event_bus))),
            node_handler: self
                .node_handler
                .unwrap_or_else(|| Arc::new(NodeOperationHandler::new(max_groups_per_node))),
            routing_handler: self
                .routing_handler
                .unwrap_or_else(|| Arc::new(RoutingOperationHandler::new())),
        }
    }
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}
