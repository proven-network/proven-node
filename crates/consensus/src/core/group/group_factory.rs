//! Factory for creating consensus groups
//!
//! This module provides a factory pattern for creating consensus groups,
//! encapsulating the complex creation logic and dependencies.

use super::{
    GroupConfig, GroupConsensusTypeConfig, GroupDependencies, groups_manager::LocalConsensusGroup,
};
use crate::core::state_machine::{
    LocalStateMachine as StorageBackedLocalState, group::GroupStateMachine,
};
use crate::{
    ConsensusGroupId,
    core::group::UnifiedGroupStorage,
    core::stream::{UnifiedStreamManager, create_stream_manager_with_backend},
    error::{ConsensusResult, Error},
};

use std::sync::Arc;

use openraft::Raft;
use proven_governance::Governance;
use proven_topology::{Node, NodeId};
use proven_transport::Transport;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Factory for creating consensus groups
pub struct GroupFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    config: GroupConfig,
    dependencies: Arc<GroupDependencies<T, G>>,
}

impl<T, G> GroupFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new group factory
    pub fn new(config: GroupConfig, dependencies: Arc<GroupDependencies<T, G>>) -> Self {
        Self {
            config,
            dependencies,
        }
    }

    /// Create a new consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<LocalConsensusGroup> {
        info!(
            "Creating consensus group {:?} with members: {:?}",
            group_id, members
        );

        // Create stream manager
        let stream_manager = self.create_stream_manager(group_id)?;

        // Create local state
        let local_state = Arc::new(RwLock::new(StorageBackedLocalState::new(
            group_id,
            stream_manager.clone(),
        )));

        // Create storage
        let storage = self.create_storage(group_id, local_state.clone()).await?;

        // Create Raft instance
        let raft = self
            .create_raft_instance(group_id, storage.clone(), self.config.node_id.clone())
            .await?;

        Ok(LocalConsensusGroup {
            raft: Arc::new(raft),
            _local_storage: storage,
            stream_manager,
            _local_state: local_state,
            id: group_id,
            members,
        })
    }

    /// Create a new consensus group with state machine using handlers
    pub async fn create_group_with_handlers(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<LocalConsensusGroup> {
        info!(
            "Creating consensus group {:?} with handler-based state machine",
            group_id
        );

        // Create stream manager
        let stream_manager = self.create_stream_manager(group_id)?;

        // Create local state for storage
        let local_state = Arc::new(RwLock::new(StorageBackedLocalState::new(
            group_id,
            stream_manager.clone(),
        )));

        // Create handler-based state machine
        let state_machine = Arc::new(GroupStateMachine::new(
            Arc::new(StorageBackedLocalState::new(
                group_id,
                stream_manager.clone(),
            )),
            group_id,
        ));

        // Create storage with the new state machine
        let storage = self
            .create_storage_with_state_machine(group_id, state_machine)
            .await?;

        // Create Raft instance
        let raft = self
            .create_raft_instance(group_id, storage.clone(), self.config.node_id.clone())
            .await?;

        Ok(LocalConsensusGroup {
            raft: Arc::new(raft),
            _local_storage: storage,
            stream_manager,
            _local_state: local_state,
            id: group_id,
            members,
        })
    }

    /// Create stream manager for a group
    fn create_stream_manager(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<UnifiedStreamManager>> {
        debug!("Creating stream manager for group {:?}", group_id);

        Ok(Arc::new(create_stream_manager_with_backend(
            group_id,
            None, // Base path will be determined by backend
            self.config.stream_storage_backend.clone(),
        )?))
    }

    /// Create storage for a group
    async fn create_storage(
        &self,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<UnifiedGroupStorage> {
        debug!("Creating storage for group {:?}", group_id);

        self.dependencies
            .storage_factory
            .create_local_storage(group_id, local_state)
            .await
    }

    /// Create storage with a custom state machine
    async fn create_storage_with_state_machine(
        &self,
        group_id: ConsensusGroupId,
        state_machine: Arc<GroupStateMachine>,
    ) -> ConsensusResult<UnifiedGroupStorage> {
        debug!(
            "Creating storage with state machine for group {:?}",
            group_id
        );

        // Now use the storage factory's new method to create storage with state machine
        self.dependencies
            .storage_factory
            .create_storage_with_state_machine(group_id, state_machine)
            .await
    }

    /// Create Raft instance for a group
    async fn create_raft_instance(
        &self,
        group_id: ConsensusGroupId,
        storage: UnifiedGroupStorage,
        node_id: NodeId,
    ) -> ConsensusResult<Raft<GroupConsensusTypeConfig>> {
        debug!("Creating Raft instance for group {:?}", group_id);

        // Create network factory
        let network_factory = crate::core::group::group_network_adaptor::GroupNetworkFactory::new(
            self.dependencies.network_manager.clone(),
            group_id,
        );

        // Create Raft instance
        let raft = Raft::new(
            node_id,
            self.config.raft_config.clone(),
            network_factory,
            storage.clone(),
            storage,
        )
        .await
        .map_err(|e| Error::Raft(format!("Failed to create Raft instance: {e}")))?;

        Ok(raft)
    }

    /// Initialize a group as coordinator
    pub async fn initialize_as_coordinator(
        &self,
        group: &LocalConsensusGroup,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        info!(
            "Initializing group {:?} as coordinator with members: {:?}",
            group.id, members
        );

        // Get node info from topology for each member
        let mut nodes = std::collections::BTreeMap::new();
        for member_id in members {
            match self
                .dependencies
                .topology_manager
                .get_governance_node(&member_id)
                .await
            {
                Ok(Some(governance_node)) => {
                    let node = Node::from(governance_node);
                    nodes.insert(member_id, node);
                }
                Ok(None) => {
                    return Err(Error::not_found(format!(
                        "Node {member_id} not found in topology"
                    )));
                }
                Err(e) => {
                    return Err(Error::InvalidMessage(format!(
                        "Failed to get node info: {e}"
                    )));
                }
            }
        }

        // Initialize Raft
        group
            .raft
            .initialize(nodes)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize group: {e:?}")))?;

        info!(
            "Successfully initialized group {:?} as coordinator",
            group.id
        );
        Ok(())
    }
}

/// Builder for creating group factories
pub struct GroupFactoryBuilder<T, G>
where
    T: Transport,
    G: Governance,
{
    config: Option<GroupConfig>,
    dependencies: Option<Arc<GroupDependencies<T, G>>>,
}

impl<T, G> GroupFactoryBuilder<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: None,
            dependencies: None,
        }
    }

    /// Set the configuration
    pub fn config(mut self, config: GroupConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the dependencies
    pub fn dependencies(mut self, deps: Arc<GroupDependencies<T, G>>) -> Self {
        self.dependencies = Some(deps);
        self
    }

    /// Build the factory
    pub fn build(self) -> Result<GroupFactory<T, G>, &'static str> {
        Ok(GroupFactory::new(
            self.config.ok_or("config is required")?,
            self.dependencies.ok_or("dependencies are required")?,
        ))
    }
}
