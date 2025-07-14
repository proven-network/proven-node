//! Global consensus layer management
//!
//! This module handles all global consensus operations including cluster management,
//! stream allocation, and global state maintenance.

use crate::{
    ConsensusGroupId,
    core::global::{GlobalConsensusTypeConfig, GlobalState, StreamConfig},
    error::{ConsensusResult, Error},
    operations::{
        GlobalOperation, GroupOperation, NodeOperation, StreamManagementOperation,
        handlers::GlobalOperationResponse,
    },
};
use maplit::btreemap;
use openraft::Raft;
use proven_topology::NodeId;
use std::{collections::HashSet, sync::Arc};
use tracing::{debug, info};

/// Type alias for global Raft instance
type GlobalRaft = Raft<GlobalConsensusTypeConfig>;

/// Global consensus layer manager
pub struct GlobalConsensusLayer {
    /// Node ID
    node_id: NodeId,
    /// Global Raft instance
    global_raft: Arc<GlobalRaft>,
    /// Global state
    global_state: Arc<GlobalState>,
}

impl GlobalConsensusLayer {
    /// Create a new global consensus layer
    pub fn new(
        node_id: NodeId,
        global_raft: Arc<GlobalRaft>,
        global_state: Arc<GlobalState>,
    ) -> Self {
        Self {
            node_id,
            global_raft,
            global_state,
        }
    }

    /// Get global state reference
    pub fn global_state(&self) -> Arc<GlobalState> {
        self.global_state.clone()
    }

    /// Get node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Check if this node is the global leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.global_raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id.clone())
    }

    /// Submit a global consensus request
    pub async fn submit_request(
        &self,
        request: GlobalOperation,
    ) -> ConsensusResult<GlobalOperationResponse> {
        debug!("Submitting global request: {:?}", request);

        // Check if we're the leader
        if !self.is_leader().await {
            return Err(Error::NotLeader { leader: None });
        }

        // Submit to Raft
        let response = self
            .global_raft
            .client_write(request)
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;

        Ok(response.data)
    }

    /// Create a stream in global state
    pub async fn create_stream(
        &self,
        name: &str,
        config: &StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Creating stream {} in group {:?}", name, group_id);

        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Create {
            name: name.to_string(),
            config: config.clone(),
            group_id,
        });

        self.submit_request(operation).await
    }

    /// Delete a stream from global state
    pub async fn delete_stream(&self, name: &str) -> ConsensusResult<GlobalOperationResponse> {
        info!("Deleting stream {}", name);

        let operation = GlobalOperation::StreamManagement(StreamManagementOperation::Delete {
            name: name.to_string(),
        });

        self.submit_request(operation).await
    }

    /// Create a consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Creating group {:?} with members {:?}", group_id, members);

        let operation = GlobalOperation::Group(GroupOperation::Create {
            group_id,
            initial_members: members,
        });

        self.submit_request(operation).await
    }

    /// Delete a consensus group
    pub async fn delete_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Deleting group {:?}", group_id);

        let operation = GlobalOperation::Group(GroupOperation::Delete { group_id });

        self.submit_request(operation).await
    }

    /// Add a node to a group
    pub async fn add_node_to_group(
        &self,
        node_id: NodeId,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Adding node {} to group {:?}", node_id, group_id);

        let operation = GlobalOperation::Node(NodeOperation::AssignToGroup { node_id, group_id });

        self.submit_request(operation).await
    }

    /// Remove a node from a group
    pub async fn remove_node_from_group(
        &self,
        node_id: NodeId,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        info!("Removing node {} from group {:?}", node_id, group_id);

        let operation = GlobalOperation::Node(NodeOperation::RemoveFromGroup { node_id, group_id });

        self.submit_request(operation).await
    }

    /// Initialize single node cluster
    pub async fn initialize_single_node(&self) -> ConsensusResult<()> {
        info!("Initializing single-node global cluster");

        // Initialize Raft as single node
        // Create a dummy GovernanceNode for initialization
        let governance_node = proven_governance::GovernanceNode {
            public_key: *self.node_id.verifying_key(),
            origin: "http://127.0.0.1:0".to_string(), // Will be updated by network layer
            availability_zone: "local".to_string(),
            region: "local".to_string(),
            specializations: HashSet::new(),
        };

        self.global_raft
            .initialize(btreemap! {
                self.node_id.clone() => proven_topology::Node::from(governance_node)
            })
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize: {e}")))?;

        Ok(())
    }

    /// Get Raft metrics
    pub async fn metrics(
        &self,
    ) -> openraft::RaftMetrics<crate::core::global::GlobalConsensusTypeConfig> {
        self.global_raft.metrics().borrow().clone()
    }

    /// Get the global Raft instance for message handling
    pub fn global_raft(&self) -> Arc<GlobalRaft> {
        self.global_raft.clone()
    }
}
