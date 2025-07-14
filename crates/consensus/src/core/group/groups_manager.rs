//! Groups manager for local consensus groups
//!
//! This module provides centralized management of local consensus groups,
//! including creation, lifecycle management, and operations routing.

use super::{
    GroupConfig, GroupConfigBuilder, GroupConsensusTypeConfig, GroupDependencies, GroupFactory,
    GroupStreamOperation,
    group_allocator::{GroupAllocations, GroupAllocator, GroupAllocatorConfig},
    migration::{StreamMigrationConfig, StreamMigrationCoordinator},
};
use crate::core::state_machine::LocalStateMachine as StorageBackedLocalState;
use crate::operations::handlers::GroupStreamOperationResponse;
use crate::{
    ConsensusGroupId,
    core::group::{GroupStorageFactory, UnifiedGroupStorage},
    core::state_machine::group::GroupStateMachine,
    core::stream::{StreamStorageBackend, UnifiedStreamManager},
    error::{ConsensusResult, Error},
};

use std::collections::HashMap;
use std::sync::Arc;

use openraft::{Config, Raft};
use proven_governance::Governance;
use proven_network::{NetworkManager, Transport};
use proven_topology::{NodeId, TopologyManager};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Manages multiple local consensus groups on a single node
pub struct GroupsManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Map of group IDs to their consensus groups
    groups: Arc<RwLock<HashMap<ConsensusGroupId, LocalConsensusGroup>>>,

    /// Group factory for creating new groups
    group_factory: Arc<GroupFactory<T, G>>,

    /// Configuration for groups
    config: GroupConfig,

    /// Dependencies for group operations
    dependencies: Arc<GroupDependencies<T, G>>,

    /// Group allocator for intelligent group management
    allocator: Option<Arc<GroupAllocator<G>>>,

    /// Stream migration coordinator
    stream_migration: Option<Arc<StreamMigrationCoordinator>>,
}

/// A single local consensus group instance
pub struct LocalConsensusGroup {
    /// The Raft instance for this group
    pub raft: Arc<Raft<GroupConsensusTypeConfig>>,
    /// Local storage (combines Raft storage and state machine)
    pub _local_storage: UnifiedGroupStorage,
    /// Stream manager for per-stream storage
    pub stream_manager: Arc<UnifiedStreamManager>,
    /// Local state machine
    pub _local_state: Arc<RwLock<StorageBackedLocalState>>,
    /// Group metadata
    pub id: ConsensusGroupId,
    /// Members of this group
    pub members: Vec<NodeId>,
}

impl<T, G> GroupsManager<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new groups manager
    pub fn new(
        node_id: NodeId,
        local_base_config: Config,
        local_storage_factory: Box<
            dyn GroupStorageFactory<
                    Storage = UnifiedGroupStorage,
                    LogStorage = UnifiedGroupStorage,
                    StateMachine = GroupStateMachine,
                >,
        >,
        stream_storage_backend: StreamStorageBackend,
        network_manager: Arc<NetworkManager<T, G>>,
        topology: Arc<TopologyManager<G>>,
    ) -> Self {
        // Build configuration
        let config = GroupConfigBuilder::new()
            .node_id(node_id)
            .raft_config(Arc::new(local_base_config))
            .stream_storage_backend(stream_storage_backend)
            .build()
            .expect("Failed to build group config");

        // Build dependencies
        let dependencies = Arc::new(GroupDependencies {
            storage_factory: local_storage_factory,
            network_manager,
            topology_manager: topology,
        });

        // Create factory
        let group_factory = Arc::new(GroupFactory::new(config.clone(), dependencies.clone()));

        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            group_factory,
            config,
            dependencies,
            allocator: None,
            stream_migration: None,
        }
    }

    /// Set the group allocator for intelligent group management
    pub fn with_allocator(mut self, allocator: Arc<GroupAllocator<G>>) -> Self {
        self.allocator = Some(allocator);
        self
    }

    /// Create allocator with default config
    pub fn with_default_allocator(mut self) -> Self {
        let allocator = Arc::new(GroupAllocator::new(
            GroupAllocatorConfig::default(),
            self.dependencies.topology_manager.clone(),
        ));
        self.allocator = Some(allocator);
        self
    }

    /// Set the stream migration coordinator
    pub fn with_stream_migration(mut self, coordinator: Arc<StreamMigrationCoordinator>) -> Self {
        self.stream_migration = Some(coordinator);
        self
    }

    /// Create stream migration coordinator with default config
    pub fn with_default_stream_migration(mut self) -> Self {
        let coordinator = Arc::new(StreamMigrationCoordinator::new(
            StreamMigrationConfig::default(),
        ));
        self.stream_migration = Some(coordinator);
        self
    }

    /// Initialize a new local consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if groups.contains_key(&group_id) {
            return Err(Error::already_exists(format!(
                "Local group {group_id:?} already exists"
            )));
        }

        info!(
            "Creating local consensus group {:?} with members: {:?}",
            group_id, members
        );

        // Use factory to create the group
        let group = self
            .group_factory
            .create_group_with_handlers(group_id, members)
            .await?;

        groups.insert(group_id, group);

        info!("Successfully created local consensus group {:?}", group_id);
        Ok(())
    }

    /// Initialize a new local consensus group without handlers (legacy mode)
    pub async fn create_group_legacy(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if groups.contains_key(&group_id) {
            return Err(Error::already_exists(format!(
                "Local group {group_id:?} already exists"
            )));
        }

        warn!(
            "Creating legacy consensus group {:?} without handlers",
            group_id
        );

        // Use factory to create the group without handlers
        let group = self.group_factory.create_group(group_id, members).await?;

        groups.insert(group_id, group);

        info!("Successfully created legacy consensus group {:?}", group_id);
        Ok(())
    }

    /// Get a specific group
    pub async fn get_group(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<LocalConsensusGroup>> {
        let groups = self.groups.read().await;

        groups
            .get(&group_id)
            .map(|g| {
                Arc::new(LocalConsensusGroup {
                    raft: g.raft.clone(),
                    _local_storage: g._local_storage.clone(),
                    stream_manager: g.stream_manager.clone(),
                    _local_state: g._local_state.clone(),
                    id: g.id,
                    members: g.members.clone(),
                })
            })
            .ok_or_else(|| Error::not_found(format!("Consensus group {group_id:?} not found")))
    }

    /// Get the stream manager for a specific group
    pub async fn get_stream_manager(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<UnifiedStreamManager>> {
        let groups = self.groups.read().await;

        groups
            .get(&group_id)
            .map(|g| g.stream_manager.clone())
            .ok_or_else(|| Error::not_found(format!("Consensus group {group_id:?} not found")))
    }

    /// Process a local stream operation
    pub async fn process_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: GroupStreamOperation,
    ) -> ConsensusResult<GroupStreamOperationResponse> {
        let groups = self.groups.read().await;

        let group = groups
            .get(&group_id)
            .ok_or_else(|| Error::not_found(format!("Consensus group {group_id:?} not found")))?;

        // Submit operation directly to Raft
        let response = group
            .raft
            .client_write(operation)
            .await
            .map_err(|e| Error::Raft(format!("Failed to write to local Raft: {e:?}")))?;

        Ok(response.data)
    }

    /// Get all group IDs
    pub async fn get_all_group_ids(&self) -> Vec<ConsensusGroupId> {
        let groups = self.groups.read().await;
        groups.keys().cloned().collect()
    }

    /// Check if a group exists
    pub async fn has_group(&self, group_id: ConsensusGroupId) -> bool {
        let groups = self.groups.read().await;
        groups.contains_key(&group_id)
    }

    /// Get the number of groups
    pub async fn group_count(&self) -> usize {
        let groups = self.groups.read().await;
        groups.len()
    }

    /// Initialize a group as coordinator (single-node initialization)
    pub async fn initialize_as_coordinator(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        // First create the group
        self.create_group(group_id, members.clone()).await?;

        // Get the group
        let groups = self.groups.read().await;
        let group = groups
            .get(&group_id)
            .ok_or_else(|| Error::not_found(format!("Group {group_id:?} not found")))?;

        // Use factory to initialize as coordinator
        self.group_factory
            .initialize_as_coordinator(group, members)
            .await?;

        info!("Initialized group {:?} as coordinator", group_id);
        Ok(())
    }

    /// Remove a group
    pub async fn remove_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if groups.remove(&group_id).is_some() {
            info!("Removed local consensus group {:?}", group_id);
            Ok(())
        } else {
            Err(Error::not_found(format!(
                "Consensus group {group_id:?} not found"
            )))
        }
    }

    /// Get metrics for all groups
    pub async fn get_all_metrics(
        &self,
    ) -> HashMap<ConsensusGroupId, openraft::RaftMetrics<GroupConsensusTypeConfig>> {
        let groups = self.groups.read().await;

        groups
            .iter()
            .map(|(id, group)| (*id, group.raft.metrics().borrow().clone()))
            .collect()
    }

    /// Get the list of managed groups
    pub async fn get_managed_groups(&self) -> Vec<ConsensusGroupId> {
        let groups = self.groups.read().await;
        groups.keys().copied().collect()
    }

    /// Check if this node is the leader of a group
    pub async fn is_leader(&self, group_id: ConsensusGroupId) -> ConsensusResult<bool> {
        let groups = self.groups.read().await;

        if let Some(group) = groups.get(&group_id) {
            let metrics = group.raft.metrics().borrow().clone();
            Ok(metrics.current_leader == Some(self.node_id().clone()))
        } else {
            Err(Error::not_found(format!(
                "Consensus group {group_id:?} not found"
            )))
        }
    }

    /// Get the Raft instance for a group
    pub async fn get_raft(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<Raft<GroupConsensusTypeConfig>>> {
        let groups = self.groups.read().await;

        groups
            .get(&group_id)
            .map(|g| g.raft.clone())
            .ok_or_else(|| Error::not_found(format!("Consensus group {group_id:?} not found")))
    }

    /// Get the node ID for this manager
    pub fn node_id(&self) -> &NodeId {
        &self.config.node_id
    }

    /// Get the factory for advanced operations
    pub fn factory(&self) -> &Arc<GroupFactory<T, G>> {
        &self.group_factory
    }

    // ============ Allocation Methods ============

    /// Allocate groups for a new node using the allocator
    pub async fn allocate_node_to_groups(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        // Get node info from governance
        let node = self
            .dependencies
            .topology_manager
            .get_node(node_id)
            .await
            .ok_or_else(|| Error::not_found(format!("Node {node_id:?} not found in topology")))?;

        // Allocate groups for the node
        let allocated_groups = allocator.allocate_groups_for_node(node_id, &node).await?;

        // Create any new groups that were allocated
        for group_id in &allocated_groups {
            if !self.has_group(*group_id).await {
                // Get members for this group from allocator
                let allocations = allocator.get_allocations().await;
                if let Some(group) = allocations.groups.get(group_id) {
                    let members: Vec<NodeId> = group.members.iter().cloned().collect();
                    self.create_group(*group_id, members).await?;
                }
            }
        }

        Ok(allocated_groups)
    }

    /// Remove a node from all its allocated groups
    pub async fn deallocate_node_from_groups(
        &self,
        node_id: &NodeId,
    ) -> ConsensusResult<Vec<ConsensusGroupId>> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        // Remove node from allocator and get affected groups
        let affected_groups = allocator.remove_node(node_id).await?;

        // Remove groups that are now too small
        let allocations = allocator.get_allocations().await;
        for group_id in &affected_groups {
            if let Some(group) = allocations.groups.get(group_id)
                && group.members.is_empty()
            {
                self.remove_group(*group_id).await?;
            }
        }

        Ok(affected_groups)
    }

    /// Get current group allocations
    pub async fn get_allocations(&self) -> ConsensusResult<GroupAllocations> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        Ok(allocator.get_allocations().await)
    }

    /// Check if rebalancing is needed
    pub async fn needs_rebalancing(&self) -> ConsensusResult<bool> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        allocator.needs_rebalancing().await
    }

    /// Generate a rebalancing plan
    pub async fn generate_rebalancing_plan(
        &self,
    ) -> ConsensusResult<Option<super::group_allocator::RebalancingPlan>> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        allocator.generate_rebalancing_plan().await
    }

    /// Start migrating a node between groups
    pub async fn start_node_migration(
        &self,
        node_id: &NodeId,
        from_group: ConsensusGroupId,
        to_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        // Start the migration in the allocator
        allocator
            .start_node_migration(node_id, from_group, to_group)
            .await?;

        // Add node to target group if it doesn't exist
        if !self.has_group(to_group).await {
            let allocations = allocator.get_allocations().await;
            if let Some(group) = allocations.groups.get(&to_group) {
                let members: Vec<NodeId> = group.members.iter().cloned().collect();
                self.create_group(to_group, members).await?;
            }
        }

        Ok(())
    }

    /// Complete a node migration
    pub async fn complete_node_migration(&self, node_id: &NodeId) -> ConsensusResult<()> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        allocator.complete_node_migration(node_id).await
    }

    /// Check if a node is currently migrating
    pub async fn is_node_migrating(&self, node_id: &NodeId) -> ConsensusResult<bool> {
        if let Some(allocator) = &self.allocator {
            Ok(allocator.is_node_migrating(node_id).await)
        } else {
            Ok(false)
        }
    }

    /// Get recommendations for group operations
    pub async fn get_group_recommendations(
        &self,
    ) -> ConsensusResult<Vec<super::group_allocator::GroupRecommendation>> {
        let allocator = self
            .allocator
            .as_ref()
            .ok_or_else(|| Error::InvalidOperation("No allocator configured".to_string()))?;

        allocator.get_recommendations().await
    }

    // ============ Stream Migration Methods ============

    /// Start a stream migration between groups
    pub async fn start_stream_migration(
        &self,
        stream_name: String,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        let coordinator = self.stream_migration.as_ref().ok_or_else(|| {
            Error::InvalidOperation("No stream migration coordinator configured".to_string())
        })?;

        coordinator
            .start_migration(stream_name, source_group, target_group)
            .await
    }

    /// Execute the next step of a stream migration
    pub async fn execute_stream_migration_step(&self, stream_name: &str) -> ConsensusResult<()> {
        let coordinator = self.stream_migration.as_ref().ok_or_else(|| {
            Error::InvalidOperation("No stream migration coordinator configured".to_string())
        })?;

        coordinator.execute_migration_step(stream_name).await
    }

    /// Get stream migration progress
    pub async fn get_stream_migration_progress(
        &self,
        stream_name: &str,
    ) -> ConsensusResult<Option<super::migration::StreamMigrationProgress>> {
        let coordinator = self.stream_migration.as_ref().ok_or_else(|| {
            Error::InvalidOperation("No stream migration coordinator configured".to_string())
        })?;

        Ok(coordinator.get_progress(stream_name).await)
    }

    /// Get all active stream migrations
    pub async fn get_active_stream_migrations(
        &self,
    ) -> ConsensusResult<Vec<super::migration::ActiveStreamMigration>> {
        let coordinator = self.stream_migration.as_ref().ok_or_else(|| {
            Error::InvalidOperation("No stream migration coordinator configured".to_string())
        })?;

        Ok(coordinator.get_active_migrations().await)
    }

    /// Cancel a stream migration
    pub async fn cancel_stream_migration(&self, stream_name: &str) -> ConsensusResult<()> {
        let coordinator = self.stream_migration.as_ref().ok_or_else(|| {
            Error::InvalidOperation("No stream migration coordinator configured".to_string())
        })?;

        coordinator.cancel_migration(stream_name).await
    }
}

impl LocalConsensusGroup {
    /// Get the group ID
    pub fn id(&self) -> ConsensusGroupId {
        self.id
    }

    /// Get the group members
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// Check if this node is the leader of the group
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        current_metrics.state.is_leader()
    }

    /// Get current leader of the group
    pub async fn current_leader(&self) -> Option<NodeId> {
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        current_metrics.current_leader.clone()
    }
}
