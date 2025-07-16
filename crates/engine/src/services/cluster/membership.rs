//! Cluster membership manager

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use proven_topology::NodeId;

use super::types::*;

/// Join request
#[derive(Debug, Clone)]
pub struct JoinRequest {
    /// Requesting node ID
    pub node_id: NodeId,
    /// Node address
    pub address: std::net::SocketAddr,
    /// Join as learner first
    pub as_learner: bool,
}

/// Leave request
#[derive(Debug, Clone)]
pub struct LeaveRequest {
    /// Leaving node ID
    pub node_id: NodeId,
    /// Leave reason
    pub reason: String,
    /// Graceful leave
    pub graceful: bool,
}

/// Membership manager
pub struct MembershipManager {
    /// Node ID
    node_id: NodeId,
    /// Membership configuration
    config: MembershipConfig,
    /// Current members
    members: Arc<RwLock<HashMap<NodeId, NodeInfo>>>,
    /// Pending changes
    pending_changes: Arc<RwLock<Vec<MembershipChange>>>,
    /// Membership version
    version: Arc<RwLock<u64>>,
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(node_id: NodeId, config: MembershipConfig) -> Self {
        Self {
            node_id,
            config,
            members: Arc::new(RwLock::new(HashMap::new())),
            pending_changes: Arc::new(RwLock::new(Vec::new())),
            version: Arc::new(RwLock::new(0)),
        }
    }

    /// Process join request
    pub async fn process_join(&self, request: JoinRequest) -> ClusterResult<()> {
        info!("Processing join request from node {}", request.node_id);

        // Check if already a member
        let members = self.members.read().await;
        if members.contains_key(&request.node_id) {
            return Err(ClusterError::AlreadyMember);
        }
        drop(members);

        // Create membership change
        let change = MembershipChange {
            id: Uuid::new_v4().to_string(),
            change_type: MembershipChangeType::Join,
            node_id: request.node_id.clone(),
            timestamp: SystemTime::now(),
            status: MembershipStatus::Pending,
        };

        // Add to pending changes
        self.pending_changes.write().await.push(change.clone());

        // Process the join
        let result = self.execute_join(request).await;

        // Update change status
        self.update_change_status(
            &change.id,
            if result.is_ok() {
                MembershipStatus::Completed
            } else {
                MembershipStatus::Failed
            },
        )
        .await;

        result
    }

    /// Process leave request
    pub async fn process_leave(&self, request: LeaveRequest) -> ClusterResult<()> {
        info!("Processing leave request from node {}", request.node_id);

        // Check if member exists
        let members = self.members.read().await;
        if !members.contains_key(&request.node_id) {
            return Err(ClusterError::NotMember);
        }
        drop(members);

        // Create membership change
        let change = MembershipChange {
            id: Uuid::new_v4().to_string(),
            change_type: MembershipChangeType::Leave,
            node_id: request.node_id.clone(),
            timestamp: SystemTime::now(),
            status: MembershipStatus::Pending,
        };

        // Add to pending changes
        self.pending_changes.write().await.push(change.clone());

        // Process the leave
        let result = if request.graceful {
            self.execute_graceful_leave(request).await
        } else {
            self.execute_forced_leave(request).await
        };

        // Update change status
        self.update_change_status(
            &change.id,
            if result.is_ok() {
                MembershipStatus::Completed
            } else {
                MembershipStatus::Failed
            },
        )
        .await;

        result
    }

    /// Execute join
    async fn execute_join(&self, request: JoinRequest) -> ClusterResult<()> {
        debug!("Executing join for node {}", request.node_id);

        // In a real implementation:
        // 1. Add node to consensus as learner/voter
        // 2. Wait for node to catch up
        // 3. Promote if necessary

        let node_info = NodeInfo {
            node_id: request.node_id.clone(),
            address: request.address,
            state: NodeState::Active,
            role: if request.as_learner {
                NodeRole::Learner
            } else {
                NodeRole::Follower
            },
            joined_at: SystemTime::now(),
            last_seen: SystemTime::now(),
            health: NodeHealth::Healthy,
        };

        // Add to members
        self.members
            .write()
            .await
            .insert(request.node_id.clone(), node_info);

        // Increment version
        *self.version.write().await += 1;

        info!("Node {} successfully joined cluster", request.node_id);
        Ok(())
    }

    /// Execute graceful leave
    async fn execute_graceful_leave(&self, request: LeaveRequest) -> ClusterResult<()> {
        debug!("Executing graceful leave for node {}", request.node_id);

        // In a real implementation:
        // 1. Transfer leadership if leader
        // 2. Ensure data is replicated
        // 3. Remove from consensus

        // Update node state
        if let Some(node) = self.members.write().await.get_mut(&request.node_id) {
            node.state = NodeState::Leaving;
        }

        // Simulate graceful shutdown
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Remove from members
        self.members.write().await.remove(&request.node_id);

        // Increment version
        *self.version.write().await += 1;

        info!("Node {} gracefully left cluster", request.node_id);
        Ok(())
    }

    /// Execute forced leave
    async fn execute_forced_leave(&self, request: LeaveRequest) -> ClusterResult<()> {
        debug!("Executing forced leave for node {}", request.node_id);

        // Remove immediately
        self.members.write().await.remove(&request.node_id);

        // Increment version
        *self.version.write().await += 1;

        warn!("Node {} forcefully removed from cluster", request.node_id);
        Ok(())
    }

    /// Promote learner to voter
    pub async fn promote_learner(&self, node_id: &NodeId) -> ClusterResult<()> {
        let mut members = self.members.write().await;

        if let Some(node) = members.get_mut(node_id) {
            if node.role != NodeRole::Learner {
                return Err(ClusterError::InvalidState(
                    "Node is not a learner".to_string(),
                ));
            }

            node.role = NodeRole::Follower;
            *self.version.write().await += 1;

            info!("Node {} promoted from learner to voter", node_id);
            Ok(())
        } else {
            Err(ClusterError::NotMember)
        }
    }

    /// Update change status
    async fn update_change_status(&self, change_id: &str, status: MembershipStatus) {
        let mut changes = self.pending_changes.write().await;
        if let Some(change) = changes.iter_mut().find(|c| c.id == change_id) {
            change.status = status;
        }
    }

    /// Get current members
    pub async fn get_members(&self) -> HashMap<NodeId, NodeInfo> {
        self.members.read().await.clone()
    }

    /// Get member info
    pub async fn get_member(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.members.read().await.get(node_id).cloned()
    }

    /// Get pending changes
    pub async fn get_pending_changes(&self) -> Vec<MembershipChange> {
        self.pending_changes.read().await.clone()
    }

    /// Get membership version
    pub async fn get_version(&self) -> u64 {
        *self.version.read().await
    }

    /// Check node health
    pub async fn check_node_health(&self, node_id: &NodeId) -> ClusterResult<NodeHealth> {
        let members = self.members.read().await;

        if let Some(node) = members.get(node_id) {
            // In a real implementation, perform actual health checks
            Ok(node.health)
        } else {
            Err(ClusterError::NotMember)
        }
    }

    /// Update node health
    pub async fn update_node_health(
        &self,
        node_id: &NodeId,
        health: NodeHealth,
    ) -> ClusterResult<()> {
        let mut members = self.members.write().await;

        if let Some(node) = members.get_mut(node_id) {
            node.health = health;
            node.last_seen = SystemTime::now();
            Ok(())
        } else {
            Err(ClusterError::NotMember)
        }
    }

    /// Mark node as failed
    pub async fn mark_node_failed(&self, node_id: &NodeId) -> ClusterResult<()> {
        let mut members = self.members.write().await;

        if let Some(node) = members.get_mut(node_id) {
            node.state = NodeState::Failed;
            node.health = NodeHealth::Unhealthy;

            error!("Node {} marked as failed", node_id);
            Ok(())
        } else {
            Err(ClusterError::NotMember)
        }
    }
}
