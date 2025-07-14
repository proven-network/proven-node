//! Unified routing functionality
//!
//! This module provides a unified routing interface for both global and group operations,
//! replacing the separate StreamRouter and ConsensusRouter implementations.

use crate::{
    ConsensusGroupId,
    core::{
        engine::groups_layer::GroupsConsensusLayer,
        global::{GlobalState, StreamConfig},
        group::GroupStreamOperation,
    },
    error::{ConsensusResult, Error},
    operations::{
        GlobalOperation,
        handlers::{GlobalOperationResponse, GroupStreamOperationResponse},
    },
};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use proven_governance::Governance;
use proven_transport::Transport;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Unified routing trait for consensus operations
#[async_trait]
pub trait ConsensusRouter: Send + Sync {
    /// Route a global operation
    async fn route_global_operation(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse>;

    /// Route a stream operation to the appropriate consensus group
    async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse>;

    /// Route a group operation directly to a specific consensus group
    async fn route_group_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse>;

    /// Get the consensus group ID for a stream
    async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId>;

    /// Check if a stream exists
    async fn stream_exists(&self, stream_name: &str) -> bool;

    /// Get all streams assigned to a specific group
    async fn get_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>>;

    /// Get stream assignment summary
    async fn get_stream_assignments(
        &self,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, Vec<String>>>;

    /// Check if this node can process operations for a stream
    async fn can_process_stream(&self, stream_name: &str) -> ConsensusResult<bool>;

    /// Get routing information for debugging
    async fn get_routing_info(&self, stream_name: &str) -> ConsensusResult<StreamRoutingInfo>;

    /// Create a stream (global operation)
    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse>;

    /// Delete a stream (global operation)
    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<GlobalOperationResponse>;
}

/// Unified operation type that can be routed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusOperation {
    /// Global operation
    Global(GlobalOperation),
    /// Group operation for a specific stream
    Stream {
        /// Stream name
        stream_name: String,
        /// The operation
        operation: GroupStreamOperation,
    },
    /// Group operation for a specific group
    Group {
        /// Target consensus group
        group_id: ConsensusGroupId,
        /// The operation
        operation: GroupStreamOperation,
    },
}

/// Unified operation response
#[derive(Debug, Clone)]
pub enum ConsensusOperationResponse {
    /// Global operation response
    Global(GlobalOperationResponse),
    /// Group operation response
    Group(GroupStreamOperationResponse),
}

/// Group health information
#[derive(Debug, Clone)]
pub struct GroupHealth {
    /// Whether the group has quorum
    pub has_quorum: bool,
    /// Number of available nodes
    pub available_nodes: usize,
    /// Number of required nodes for quorum
    pub required_nodes: usize,
}

/// Unified router implementation that handles both global and group operations
pub struct UnifiedRouter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Global state reference
    global_state: Arc<GlobalState>,
    /// Groups consensus layer
    groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,
    /// Reference to global consensus layer for global operations
    global_layer: Option<Arc<dyn GlobalOperationHandler + Send + Sync>>,
}

/// Trait for handling global operations
#[async_trait]
pub trait GlobalOperationHandler: Send + Sync {
    /// Submit a global operation
    async fn submit_global_request(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse>;

    /// Create a stream with cross-layer coordination
    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse>;

    /// Delete a stream with cross-layer coordination
    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<GlobalOperationResponse>;
}

/// For backward compatibility, re-export UnifiedRouter as StreamRouter
pub type StreamRouter<T, G> = UnifiedRouter<T, G>;

impl<T, G> UnifiedRouter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new unified router
    pub fn new(
        global_state: Arc<GlobalState>,
        groups_layer: Arc<RwLock<GroupsConsensusLayer<T, G>>>,
    ) -> Self {
        Self {
            global_state,
            groups_layer,
            global_layer: None,
        }
    }

    /// Set the global operation handler
    pub fn with_global_handler(
        mut self,
        handler: Arc<dyn GlobalOperationHandler + Send + Sync>,
    ) -> Self {
        self.global_layer = Some(handler);
        self
    }

    /// Route a stream operation to the appropriate consensus group
    pub async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        debug!(
            "Routing operation for stream '{}': {:?}",
            stream_name, operation
        );

        // Get the stream info from global state
        let stream_info = self
            .global_state
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        info!(
            "Stream '{}' is assigned to group {:?}",
            stream_name, stream_info.group_id
        );

        // Route to the appropriate group
        self.groups_layer
            .read()
            .await
            .process_operation(stream_info.group_id, operation)
            .await
    }

    /// Get the consensus group ID for a stream
    pub async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId> {
        let stream_info = self
            .global_state
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        Ok(stream_info.group_id)
    }

    /// Check if a stream exists
    pub async fn stream_exists(&self, stream_name: &str) -> bool {
        self.global_state.get_stream(stream_name).await.is_some()
    }

    /// Get all streams assigned to a specific group
    pub async fn get_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>> {
        Ok(self.global_state.get_streams_in_group(group_id).await)
    }

    /// Get stream assignment summary (which streams are in which groups)
    pub async fn get_stream_assignments(
        &self,
    ) -> ConsensusResult<std::collections::HashMap<ConsensusGroupId, Vec<String>>> {
        // Get all groups first
        let groups = self.global_state.get_all_groups().await;
        let mut assignments = std::collections::HashMap::new();

        // For each group, get its streams
        for group in groups {
            let streams = self.global_state.get_streams_in_group(group.id).await;
            if !streams.is_empty() {
                assignments.insert(group.id, streams);
            }
        }

        Ok(assignments)
    }

    /// Check if a node can process operations for a stream
    pub async fn can_process_stream(&self, stream_name: &str) -> ConsensusResult<bool> {
        // Get the stream's group
        let group_id = self.get_stream_group(stream_name).await?;

        // Check if we have this group
        Ok(self.groups_layer.read().await.has_group(group_id).await)
    }

    /// Get routing information for debugging
    pub async fn get_routing_info(&self, stream_name: &str) -> ConsensusResult<StreamRoutingInfo> {
        let stream_info = self
            .global_state
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        let has_group = self
            .groups_layer
            .read()
            .await
            .has_group(stream_info.group_id)
            .await;

        let is_leader = if has_group {
            self.groups_layer
                .read()
                .await
                .groups_manager()
                .read()
                .await
                .is_leader(stream_info.group_id)
                .await
                .unwrap_or(false)
        } else {
            false
        };

        Ok(StreamRoutingInfo {
            stream_name: stream_name.to_string(),
            group_id: stream_info.group_id,
            has_group,
            is_leader,
        })
    }
}

#[async_trait]
impl<T, G> ConsensusRouter for UnifiedRouter<T, G>
where
    T: Transport,
    G: Governance,
{
    async fn route_global_operation(
        &self,
        operation: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse> {
        match &self.global_layer {
            Some(handler) => handler.submit_global_request(operation).await,
            None => Err(Error::InvalidState(
                "Global operation handler not configured".to_string(),
            )),
        }
    }

    async fn route_stream_operation(
        &self,
        stream_name: &str,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        debug!(
            "Routing operation for stream '{}': {:?}",
            stream_name, operation
        );

        // Get the stream info from global state
        let stream_info = self
            .global_state
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        info!(
            "Stream '{}' is assigned to group {:?}",
            stream_name, stream_info.group_id
        );

        // Route to the appropriate group
        self.groups_layer
            .read()
            .await
            .process_operation(stream_info.group_id, operation)
            .await
    }

    async fn route_group_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        self.groups_layer
            .read()
            .await
            .process_operation(group_id, operation)
            .await
    }

    async fn get_stream_group(&self, stream_name: &str) -> ConsensusResult<ConsensusGroupId> {
        let stream_info = self
            .global_state
            .get_stream(stream_name)
            .await
            .ok_or_else(|| Error::not_found(format!("Stream '{stream_name}' not found")))?;

        Ok(stream_info.group_id)
    }

    async fn stream_exists(&self, stream_name: &str) -> bool {
        self.global_state.get_stream(stream_name).await.is_some()
    }

    async fn get_streams_in_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Vec<String>> {
        Ok(self.global_state.get_streams_in_group(group_id).await)
    }

    async fn get_stream_assignments(
        &self,
    ) -> ConsensusResult<HashMap<ConsensusGroupId, Vec<String>>> {
        // Get all groups first
        let groups = self.global_state.get_all_groups().await;
        let mut assignments = HashMap::new();

        // For each group, get its streams
        for group in groups {
            let streams = self.global_state.get_streams_in_group(group.id).await;
            if !streams.is_empty() {
                assignments.insert(group.id, streams);
            }
        }

        Ok(assignments)
    }

    async fn can_process_stream(&self, stream_name: &str) -> ConsensusResult<bool> {
        // Get the stream's group
        let group_id = self.get_stream_group(stream_name).await?;

        // Check if we have this group
        Ok(self.groups_layer.read().await.has_group(group_id).await)
    }

    async fn get_routing_info(&self, stream_name: &str) -> ConsensusResult<StreamRoutingInfo> {
        self.get_routing_info(stream_name).await
    }

    async fn create_stream(
        &self,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        match &self.global_layer {
            Some(handler) => handler.create_stream(name, config, group_id).await,
            None => Err(Error::InvalidState(
                "Global operation handler not configured".to_string(),
            )),
        }
    }

    async fn delete_stream(&self, stream_name: &str) -> ConsensusResult<GlobalOperationResponse> {
        match &self.global_layer {
            Some(handler) => handler.delete_stream(stream_name).await,
            None => Err(Error::InvalidState(
                "Global operation handler not configured".to_string(),
            )),
        }
    }
}

/// Stream routing information for debugging
#[derive(Debug, Clone)]
pub struct StreamRoutingInfo {
    /// Stream name
    pub stream_name: String,
    /// Assigned group ID
    pub group_id: ConsensusGroupId,
    /// Whether this node has the group
    pub has_group: bool,
    /// Whether this node is the leader of the group
    pub is_leader: bool,
}
