//! Service lifecycle and management traits

use async_trait::async_trait;
use std::sync::Arc;

use crate::error::ConsensusResult;
use crate::foundation::StreamState;
use crate::foundation::types::{ConsensusGroupId, StreamName};
use proven_topology::NodeId;

/// Service lifecycle management
#[async_trait]
pub trait ServiceLifecycle: Send + Sync {
    /// Initialize the service
    async fn initialize(&self) -> ConsensusResult<()>;

    /// Start the service
    async fn start(&self) -> ConsensusResult<()>;

    /// Stop the service
    async fn stop(&self) -> ConsensusResult<()>;

    /// Check if service is healthy
    async fn is_healthy(&self) -> bool;

    /// Get service status
    async fn status(&self) -> ServiceStatus;
}

/// Service status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    /// Service is not initialized
    Uninitialized,
    /// Service is initializing
    Initializing,
    /// Service is starting
    Starting,
    /// Service is running
    Running,
    /// Service is stopping
    Stopping,
    /// Service is stopped
    Stopped,
    /// Service has failed
    Failed,
}

/// Service coordinator for managing multiple services
#[async_trait]
pub trait ServiceCoordinator: Send + Sync {
    /// Register a service
    async fn register_service(
        &self,
        name: String,
        service: Arc<dyn ServiceLifecycle>,
    ) -> ConsensusResult<()>;

    /// Start all services
    async fn start_all(&self) -> ConsensusResult<()>;

    /// Stop all services
    async fn stop_all(&self) -> ConsensusResult<()>;

    /// Get status of all services
    async fn get_status(&self) -> Vec<(String, ServiceStatus)>;
}

/// Group management operations
#[async_trait]
pub trait GroupManager: Send + Sync {
    /// Create a new consensus group
    async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()>;

    /// Dissolve a consensus group
    async fn dissolve_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()>;

    /// Add member to group
    async fn add_member(&self, group_id: ConsensusGroupId, node_id: NodeId) -> ConsensusResult<()>;

    /// Remove member from group
    async fn remove_member(
        &self,
        group_id: ConsensusGroupId,
        node_id: NodeId,
    ) -> ConsensusResult<()>;

    /// Get group members
    async fn get_members(&self, group_id: ConsensusGroupId) -> ConsensusResult<Vec<NodeId>>;

    /// Check if node is member of group
    async fn is_member(&self, group_id: ConsensusGroupId, node_id: &NodeId) -> bool;
}

/// Stream management operations
#[async_trait]
pub trait StreamManager: Send + Sync {
    /// Create a new stream
    async fn create_stream(
        &self,
        name: StreamName,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<()>;

    /// Delete a stream
    async fn delete_stream(&self, name: &StreamName) -> ConsensusResult<()>;

    /// Get stream state
    async fn get_stream_state(&self, name: &StreamName) -> ConsensusResult<Option<StreamState>>;

    /// List all streams
    async fn list_streams(&self) -> ConsensusResult<Vec<StreamName>>;

    /// Trim a stream
    async fn trim_stream(&self, name: &StreamName, up_to_seq: u64) -> ConsensusResult<()>;
}
