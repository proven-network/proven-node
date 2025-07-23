//! Core traits that define the consensus system interfaces

use async_trait::async_trait;
use std::sync::Arc;

use crate::services::stream::{StreamName, StreamState};

use super::types::*;
use crate::error::ConsensusResult;

/// Trait for consensus layers (global and group)
#[async_trait]
pub trait ConsensusLayer: Send + Sync {
    /// The type of operations this layer handles
    type Operation: Send + Sync;

    /// The type of state this layer maintains
    type State: Send + Sync;

    /// Initialize the consensus layer
    async fn initialize(&self) -> ConsensusResult<()>;

    /// Propose an operation
    async fn propose(&self, operation: Self::Operation) -> ConsensusResult<OperationId>;

    /// Get current state
    async fn get_state(&self) -> ConsensusResult<Arc<Self::State>>;

    /// Check if this node is the leader
    async fn is_leader(&self) -> bool;

    /// Get current term
    async fn current_term(&self) -> Term;

    /// Get current role
    async fn current_role(&self) -> ConsensusRole;
}

/// Trait for state storage
#[async_trait]
pub trait StateStore: Send + Sync {
    /// The type of state being stored
    type State: Send + Sync;

    /// Save state
    async fn save(&self, state: &Self::State) -> ConsensusResult<()>;

    /// Load state
    async fn load(&self) -> ConsensusResult<Option<Self::State>>;

    /// Apply a state update
    async fn apply_update<F>(&self, update: F) -> ConsensusResult<()>
    where
        F: FnOnce(&mut Self::State) + Send;
}

/// Trait for handling operations
#[async_trait]
pub trait OperationHandler: Send + Sync {
    /// The type of operations this handler processes
    type Operation: Send + Sync;

    /// The type of response returned
    type Response: Send + Sync;

    /// Handle an operation
    async fn handle(
        &self,
        operation: Self::Operation,
        is_replay: bool,
    ) -> ConsensusResult<Self::Response>;

    /// Validate an operation before processing
    async fn validate(&self, operation: &Self::Operation) -> ConsensusResult<()>;
}

/// Trait for event handling
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// The type of events this handler processes
    type Event: Send + Sync;

    /// Handle an event
    async fn handle_event(&self, event: Self::Event) -> ConsensusResult<()>;

    /// Get event types this handler is interested in
    fn subscribed_events(&self) -> Vec<String>;
}

/// Trait for service lifecycle management
#[async_trait]
pub trait ServiceLifecycle: Send + Sync {
    /// Start the service
    async fn start(&self) -> ConsensusResult<()>;

    /// Stop the service
    async fn stop(&self) -> ConsensusResult<()>;

    /// Check if service is running
    async fn is_running(&self) -> bool;

    /// Get service health status
    async fn health_check(&self) -> ConsensusResult<ServiceHealth>;
}

/// Service health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceHealth {
    /// Service name
    pub name: String,
    /// Health status
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
    /// Subsystem health
    pub subsystems: Vec<SubsystemHealth>,
}

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Service is healthy
    Healthy,
    /// Service is degraded but operational
    Degraded,
    /// Service is unhealthy
    Unhealthy,
}

/// Subsystem health
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubsystemHealth {
    /// Subsystem name
    pub name: String,
    /// Health status
    pub status: HealthStatus,
    /// Optional message
    pub message: Option<String>,
}

/// Trait for coordination between services
#[async_trait]
pub trait ServiceCoordinator: Send + Sync {
    /// Register a service
    async fn register_service(
        &self,
        name: String,
        service: Arc<dyn ServiceLifecycle>,
    ) -> ConsensusResult<()>;

    /// Start all registered services
    async fn start_all(&self) -> ConsensusResult<()>;

    /// Stop all registered services
    async fn stop_all(&self) -> ConsensusResult<()>;

    /// Get service by name
    async fn get_service(&self, name: &str) -> Option<Arc<dyn ServiceLifecycle>>;
}

/// Trait for consensus group management
#[async_trait]
pub trait GroupManager: Send + Sync {
    /// Create a new consensus group
    async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<proven_topology::NodeId>,
    ) -> ConsensusResult<()>;

    /// Dissolve a consensus group
    async fn dissolve_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()>;

    /// Add member to group
    async fn add_member(
        &self,
        group_id: ConsensusGroupId,
        member: proven_topology::NodeId,
    ) -> ConsensusResult<()>;

    /// Remove member from group
    async fn remove_member(
        &self,
        group_id: ConsensusGroupId,
        member: proven_topology::NodeId,
    ) -> ConsensusResult<()>;

    /// Get group state
    async fn get_group_state(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupState2>>;
}

/// Trait for stream management
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

    /// Migrate stream to different group
    async fn migrate_stream(
        &self,
        name: &StreamName,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()>;

    /// Get stream state
    async fn get_stream_state(&self, name: &StreamName) -> ConsensusResult<Option<StreamState>>;
}
