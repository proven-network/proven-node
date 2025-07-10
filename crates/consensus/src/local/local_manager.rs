use super::LocalStreamOperation;
use super::StorageBackedLocalState;
use super::group_storage::{
    LocalStorageFactory, UnifiedLocalStorage, create_local_storage_factory,
};
use super::network_factory::LocalNetworkRegistry;
use super::stream_storage::{
    StreamConfig, StreamStorageBackend, UnifiedStreamManager, create_stream_manager_with_backend,
};
use super::{LocalRequest, LocalTypeConfig};
use crate::allocation::ConsensusGroupId;
use crate::config::StorageConfig;
use crate::error::{ConsensusResult, Error};
use crate::global::global_manager::GlobalManager;
use crate::node::Node;
use crate::node_id::NodeId;

use std::collections::HashMap;
use std::sync::Arc;

use openraft::{Config, Raft, RaftMetrics};
use tokio::sync::RwLock;

/// Manages multiple local consensus groups on a single node
pub struct LocalConsensusManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Map of group IDs to their Raft instances
    groups: Arc<RwLock<HashMap<ConsensusGroupId, LocalConsensusGroup>>>,
    /// Node ID of this manager
    node_id: NodeId,
    /// Base configuration for Raft instances
    base_config: Arc<Config>,
    /// Network registry for local groups
    network_registry: Arc<LocalNetworkRegistry>,
    /// Reference to the global consensus manager
    global_manager: Arc<GlobalManager<G, A>>,
    /// Local storage factory
    local_storage_factory: Box<dyn LocalStorageFactory<Storage = UnifiedLocalStorage>>,
    /// Base path for stream storage (if using file-based storage)
    stream_storage_base_path: Option<std::path::PathBuf>,
    /// Storage backend for streams
    stream_storage_backend: StreamStorageBackend,
}

/// A single local consensus group instance
struct LocalConsensusGroup {
    /// The Raft instance for this group
    raft: Arc<Raft<LocalTypeConfig>>,
    /// Local storage (combines Raft storage and state machine)
    _local_storage: UnifiedLocalStorage,
    /// Stream manager for per-stream storage
    stream_manager: Arc<UnifiedStreamManager>,
    /// Local state machine
    _local_state: Arc<RwLock<StorageBackedLocalState>>,
    /// Group metadata
    _id: ConsensusGroupId,
    /// Members of this group
    _members: Vec<NodeId>,
    /// Metrics for monitoring
    _metrics: RaftMetrics<LocalTypeConfig>,
    /// Migration state for this group
    migration_state: MigrationState,
}

/// Migration state for a node in a group
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationState {
    /// Node is a regular member of this group
    Active,
    /// Node is joining this group (dual membership - new group)
    Joining,
    /// Node is leaving this group (dual membership - old group)
    Leaving,
}

impl<G, A> LocalConsensusManager<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Create a new local consensus manager
    pub fn new(
        node_id: NodeId,
        base_config: Config,
        global_manager: Arc<GlobalManager<G, A>>,
    ) -> ConsensusResult<Self> {
        let storage_config = StorageConfig::Memory;
        let local_storage_factory = create_local_storage_factory(&storage_config)?;

        Ok(Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            base_config: Arc::new(base_config),
            network_registry: Arc::new(LocalNetworkRegistry::new()),
            global_manager,
            local_storage_factory,
            stream_storage_base_path: None,
            stream_storage_backend: StreamStorageBackend::default(),
        })
    }

    /// Create a new local consensus manager with storage configuration
    pub fn with_storage_config(
        node_id: NodeId,
        base_config: Config,
        global_manager: Arc<GlobalManager<G, A>>,
        storage_config: &StorageConfig,
    ) -> ConsensusResult<Self> {
        let local_storage_factory = create_local_storage_factory(storage_config)?;
        let stream_storage_base_path = match storage_config {
            StorageConfig::Memory => None,
            StorageConfig::RocksDB { path } => Some(path.clone()),
        };

        Ok(Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            base_config: Arc::new(base_config),
            network_registry: Arc::new(LocalNetworkRegistry::new()),
            global_manager,
            local_storage_factory,
            stream_storage_base_path,
            stream_storage_backend: StreamStorageBackend::default(),
        })
    }

    /// Create a new local consensus manager with a storage factory
    pub fn with_storage_factory(
        node_id: NodeId,
        base_config: Config,
        global_manager: Arc<GlobalManager<G, A>>,
        local_storage_factory: Box<dyn LocalStorageFactory<Storage = UnifiedLocalStorage>>,
    ) -> Self {
        Self::with_storage_factory_and_backend(
            node_id,
            base_config,
            global_manager,
            local_storage_factory,
            StreamStorageBackend::default(),
        )
    }

    /// Create a new local consensus manager with storage factory and backend
    pub fn with_storage_factory_and_backend(
        node_id: NodeId,
        base_config: Config,
        global_manager: Arc<GlobalManager<G, A>>,
        local_storage_factory: Box<dyn LocalStorageFactory<Storage = UnifiedLocalStorage>>,
        stream_storage_backend: StreamStorageBackend,
    ) -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            node_id,
            base_config: Arc::new(base_config),
            network_registry: Arc::new(LocalNetworkRegistry::new()),
            global_manager,
            local_storage_factory,
            stream_storage_base_path: None,
            stream_storage_backend,
        }
    }

    /// Get the network registry
    pub fn network_registry(&self) -> &Arc<LocalNetworkRegistry> {
        &self.network_registry
    }

    /// Discover which consensus groups this node belongs to
    /// In the future, this will query the global consensus state
    pub async fn discover_my_groups(&self) -> ConsensusResult<Vec<ConsensusGroupId>> {
        // TODO: Once we have a way to query global consensus state,
        // this should submit a read request to get group membership
        // For now, return empty
        Ok(vec![])
    }

    /// Get the global manager reference
    pub fn global_manager(&self) -> &Arc<GlobalManager<G, A>> {
        &self.global_manager
    }

    /// Create and start a new local consensus group
    pub async fn create_group(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if groups.contains_key(&group_id) {
            return Err(Error::already_exists(format!(
                "Consensus group {:?} already exists",
                group_id
            )));
        }

        // Create stream manager for this group
        let stream_manager = Arc::new(create_stream_manager_with_backend(
            group_id,
            self.stream_storage_base_path.clone(),
            self.stream_storage_backend.clone(),
        )?);

        // Create local state machine
        let local_state = Arc::new(RwLock::new(StorageBackedLocalState::new(
            group_id,
            stream_manager.clone(),
        )));

        // Create local storage for this group (includes both Raft storage and state machine)
        let local_storage = self
            .local_storage_factory
            .create_local_storage(group_id, local_state.clone())
            .await?;

        // Create a customized config for this group
        let mut config = (*self.base_config).clone();
        // Use shorter timeouts for local groups for faster consensus
        config.heartbeat_interval = 100;
        config.election_timeout_min = 200;
        config.election_timeout_max = 400;

        // Get or create network factory for this group
        let network_factory = self.network_registry.create_network_factory(group_id).await;

        // Register all members with the network registry
        for member in &members {
            self.network_registry
                .assign_node_to_group(member.clone(), group_id)
                .await;
        }

        // Create the Raft instance
        let raft = openraft::Raft::new(
            self.node_id.clone(),
            Arc::new(config),
            network_factory,
            local_storage.clone(),
            local_storage.clone(),
        )
        .await
        .map_err(|e| Error::Raft(format!("Failed to create Raft instance: {:?}", e)))?;

        // Get initial metrics
        let metrics = raft.metrics().borrow().clone();

        let group = LocalConsensusGroup {
            raft: Arc::new(raft),
            _local_storage: local_storage,
            stream_manager,
            _local_state: local_state,
            _id: group_id,
            _members: members,
            _metrics: metrics,
            migration_state: MigrationState::Active,
        };

        groups.insert(group_id, group);
        Ok(())
    }

    /// Remove a local consensus group
    pub async fn remove_group(&self, group_id: ConsensusGroupId) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.remove(&group_id) {
            // Shutdown the Raft instance
            group
                .raft
                .shutdown()
                .await
                .map_err(|e| Error::Raft(format!("Failed to shutdown Raft: {:?}", e)))?;

            // Clean up Raft storage for this group
            // TODO: Add cleanup method to LocalStorageFactory if needed

            // Clean up all streams in the stream manager
            group.stream_manager.cleanup_all().await?;

            Ok(())
        } else {
            Err(Error::not_found(format!(
                "Consensus group {:?} not found",
                group_id
            )))
        }
    }

    /// Get the Raft instance for a specific group
    pub async fn get_raft(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Arc<Raft<LocalTypeConfig>>> {
        let groups = self.groups.read().await;

        groups
            .get(&group_id)
            .map(|g| g.raft.clone())
            .ok_or_else(|| Error::not_found(format!("Consensus group {:?} not found", group_id)))
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
            .ok_or_else(|| Error::not_found(format!("Consensus group {:?} not found", group_id)))
    }

    /// Create a stream in a group with specific configuration
    pub async fn create_stream(
        &self,
        group_id: ConsensusGroupId,
        stream_id: &str,
        config: StreamConfig,
    ) -> ConsensusResult<()> {
        let stream_manager = self.get_stream_manager(group_id).await?;
        stream_manager
            .create_stream(stream_id.to_string(), config)
            .await
            .map_err(|e| Error::storage(format!("Failed to create stream: {}", e)))?;
        Ok(())
    }

    /// Process a local stream operation
    pub async fn process_operation(
        &self,
        group_id: ConsensusGroupId,
        operation: LocalStreamOperation,
    ) -> ConsensusResult<super::LocalResponse> {
        let raft = self.get_raft(group_id).await?;

        // Create LocalRequest from the operation
        let request = LocalRequest { operation };

        // Submit to Raft
        let response = raft
            .client_write(request)
            .await
            .map_err(|e| Error::Raft(format!("Failed to write to Raft: {:?}", e)))?;

        Ok(response.data)
    }

    /// Join a consensus group as a learner
    pub async fn join_group(
        &self,
        group_id: ConsensusGroupId,
        _leader_id: NodeId,
        leader_node: Node,
    ) -> ConsensusResult<()> {
        let raft = self.get_raft(group_id).await?;

        // Add this node as a learner using the leader's node info
        raft.add_learner(self.node_id.clone(), leader_node, true)
            .await
            .map_err(|e| Error::Raft(format!("Failed to add learner: {:?}", e)))?;

        Ok(())
    }

    /// Join a consensus group as part of a migration (dual membership)
    pub async fn join_group_for_migration(
        &self,
        group_id: ConsensusGroupId,
        members: Vec<NodeId>,
    ) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if groups.contains_key(&group_id) {
            return Err(Error::already_exists(format!(
                "Already a member of group {:?}",
                group_id
            )));
        }

        // Create stream manager for this group
        let stream_manager = Arc::new(create_stream_manager_with_backend(
            group_id,
            self.stream_storage_base_path.clone(),
            self.stream_storage_backend.clone(),
        )?);

        // Create local state machine
        let local_state = Arc::new(RwLock::new(StorageBackedLocalState::new(
            group_id,
            stream_manager.clone(),
        )));

        // Create local storage for this group (includes both Raft storage and state machine)
        let local_storage = self
            .local_storage_factory
            .create_local_storage(group_id, local_state.clone())
            .await?;

        // Create a customized config for this group
        let mut config = (*self.base_config).clone();
        config.heartbeat_interval = 100;
        config.election_timeout_min = 200;
        config.election_timeout_max = 400;

        // Get or create network factory for this group
        let network_factory = self.network_registry.create_network_factory(group_id).await;

        // Register all members with the network registry
        for member in &members {
            self.network_registry
                .assign_node_to_group(member.clone(), group_id)
                .await;
        }

        // Create the Raft instance
        let raft = openraft::Raft::new(
            self.node_id.clone(),
            Arc::new(config),
            network_factory,
            local_storage.clone(),
            local_storage.clone(),
        )
        .await
        .map_err(|e| Error::Raft(format!("Failed to create Raft instance: {:?}", e)))?;

        // Get initial metrics
        let metrics = raft.metrics().borrow().clone();

        let group = LocalConsensusGroup {
            raft: Arc::new(raft),
            _local_storage: local_storage,
            stream_manager,
            _local_state: local_state,
            _id: group_id,
            _members: members,
            _metrics: metrics,
            migration_state: MigrationState::Joining,
        };

        groups.insert(group_id, group);
        Ok(())
    }

    /// Update the migration state for a group
    pub async fn update_migration_state(
        &self,
        group_id: ConsensusGroupId,
        state: MigrationState,
    ) -> ConsensusResult<()> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            group.migration_state = state;
            Ok(())
        } else {
            Err(Error::not_found(format!(
                "Consensus group {:?} not found",
                group_id
            )))
        }
    }

    /// Get all groups this node is a member of
    pub async fn get_my_groups(&self) -> Vec<(ConsensusGroupId, MigrationState)> {
        let groups = self.groups.read().await;
        groups
            .iter()
            .map(|(id, group)| (*id, group.migration_state.clone()))
            .collect()
    }

    /// Check if node is in migration (member of multiple groups)
    pub async fn is_in_migration(&self) -> bool {
        let groups = self.groups.read().await;
        groups
            .values()
            .any(|g| g.migration_state != MigrationState::Active)
    }

    /// Get metrics for all groups
    pub async fn get_all_metrics(&self) -> HashMap<ConsensusGroupId, RaftMetrics<LocalTypeConfig>> {
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
        let raft = self.get_raft(group_id).await?;
        let metrics = raft.metrics().borrow().clone();

        Ok(metrics.current_leader == Some(self.node_id.clone()))
    }

    /// Initialize a new group with this node as the only member
    pub async fn initialize_single_node_group(
        &self,
        group_id: ConsensusGroupId,
        node_info: Node,
    ) -> ConsensusResult<()> {
        // Create the group
        self.create_group(group_id, vec![self.node_id.clone()])
            .await?;

        // Get the Raft instance
        let raft = self.get_raft(group_id).await?;

        // Initialize as a single-node cluster
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(self.node_id.clone(), node_info);

        raft.initialize(nodes)
            .await
            .map_err(|e| Error::Raft(format!("Failed to initialize cluster: {:?}", e)))?;

        Ok(())
    }
}
