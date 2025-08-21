//! Handler for EnsureGroupConsensusInitialized command

use async_trait::async_trait;
use proven_storage::LogStorage;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::foundation::events::{Error as EventError, EventBus, EventMetadata, RequestHandler};
use crate::foundation::routing::RoutingTable;
use crate::services::group_consensus::commands::EnsureGroupConsensusInitialized;
use crate::services::group_consensus::types::{ConsensusLayers, States};
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager, StorageNamespace};
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

/// Handler for EnsureGroupConsensusInitialized command
pub struct EnsureGroupConsensusInitializedHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
    /// Group states
    group_states: States,
    /// Network manager
    network_manager: Arc<NetworkManager<T, G, A>>,
    /// Storage manager
    storage_manager: Arc<StorageManager<S>>,
    /// Topology manager
    topology_manager: Arc<TopologyManager<G>>,
    /// Event bus
    event_bus: Arc<EventBus>,
    /// Routing table
    routing_table: Arc<RoutingTable>,
}

impl<T, G, A, S> EnsureGroupConsensusInitializedHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        groups: ConsensusLayers<S>,
        group_states: States,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage_manager: Arc<StorageManager<S>>,
        topology_manager: Arc<TopologyManager<G>>,
        event_bus: Arc<EventBus>,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        Self {
            node_id,
            groups,
            group_states,
            network_manager,
            storage_manager,
            topology_manager,
            event_bus,
            routing_table,
        }
    }

    /// Check if a group is already initialized in persistent storage
    async fn is_group_initialized_in_storage(
        &self,
        group_id: crate::foundation::types::ConsensusGroupId,
    ) -> bool {
        // Check if the group's log storage namespace exists
        let logs_namespace = StorageNamespace::new(format!("group_{}_logs", group_id.value()));
        let storage = self.storage_manager.consensus_storage();

        // If we can get bounds, the storage exists
        match storage.bounds(&logs_namespace).await {
            Ok(Some(_)) => {
                debug!("Group {:?} already has persistent storage", group_id);
                true
            }
            Ok(None) => {
                debug!("Group {:?} has no persistent storage", group_id);
                false
            }
            Err(e) => {
                // If there's an error checking bounds, assume not initialized
                debug!("Error checking group {:?} storage: {}", group_id, e);
                false
            }
        }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<EnsureGroupConsensusInitialized>
    for EnsureGroupConsensusInitializedHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: EnsureGroupConsensusInitialized,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "EnsureGroupConsensusInitializedHandler: Ensuring group {:?} is initialized with members: {:?}",
            request.group_id, request.members
        );

        // Check if we're a member of this group
        if !request.members.contains(&self.node_id) {
            info!(
                "Not a member of group {:?}, skipping initialization",
                request.group_id
            );
            return Ok(());
        }

        // Check if group already exists in memory
        {
            let groups_guard = self.groups.read().await;
            if groups_guard.contains_key(&request.group_id) {
                info!("Group {:?} already initialized in memory", request.group_id);
                return Ok(());
            }
        }

        // Check if group exists in persistent storage
        let exists_in_storage = self.is_group_initialized_in_storage(request.group_id).await;

        // Create the group consensus layer
        use crate::consensus::group::GroupConsensusLayer;
        use crate::foundation::create_group_state_access;
        use crate::services::group_consensus::adaptor::GroupNetworkFactory;
        use crate::services::group_consensus::callbacks::GroupConsensusCallbacksImpl;

        info!(
            "Creating consensus layer for group {:?} (exists_in_storage: {})",
            request.group_id, exists_in_storage
        );

        // Create storage for the group
        let storage = self.storage_manager.consensus_storage();

        // Create state access
        let (state_reader, state_writer) = create_group_state_access();

        // Store the state reader
        self.group_states
            .write()
            .await
            .insert(request.group_id, state_reader.clone());

        // Create callbacks
        let callbacks = Arc::new(GroupConsensusCallbacksImpl::<S>::new(
            request.group_id,
            self.node_id,
            Some(self.event_bus.clone()),
        ));

        // Create Raft config (from the service config - using defaults for now)
        let raft_config = openraft::Config {
            cluster_name: format!("group-{}", request.group_id),
            election_timeout_min: 300,
            election_timeout_max: 600,
            heartbeat_interval: 100,
            max_payload_entries: 64,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            ..Default::default()
        };

        // Create network factory
        let network_factory =
            GroupNetworkFactory::new(self.network_manager.clone(), request.group_id);

        // Create the consensus layer
        let layer = Arc::new(
            GroupConsensusLayer::new(
                self.node_id,
                request.group_id,
                raft_config,
                network_factory,
                storage,
                callbacks,
                state_writer,
            )
            .await
            .map_err(|e| EventError::Internal(format!("Failed to create consensus layer: {e}")))?,
        );

        // Check if the layer is already initialized
        let is_initialized = layer.is_initialized().await;

        // Store the layer
        {
            let mut groups = self.groups.write().await;
            groups.insert(request.group_id, layer.clone());
        }

        // Start event monitoring for this group
        use crate::services::group_consensus::GroupConsensusService;
        GroupConsensusService::<T, G, A, S>::start_group_event_monitoring(
            layer.clone(),
            self.event_bus.clone(),
            self.node_id,
            request.group_id,
            self.routing_table.clone(),
        );

        // Only initialize Raft if:
        // 1. The layer is not already initialized
        // 2. We're the initializer (lowest node ID)
        if !is_initialized {
            // Get topology information for members
            let mut raft_members = std::collections::BTreeMap::new();
            for member_id in &request.members {
                if let Some(node) = self.topology_manager.get_node(member_id).await {
                    raft_members.insert(*member_id, node);
                } else {
                    error!(
                        "Member {} not found in topology for group {}",
                        member_id, request.group_id
                    );
                    return Err(EventError::Internal(format!(
                        "Member {member_id} not found in topology"
                    )));
                }
            }

            // Initialize the Raft cluster if we're the first member (lowest node ID)
            let sorted_members: Vec<_> = request.members.iter().collect();
            let is_initializer = sorted_members.first() == Some(&&self.node_id);

            if is_initializer {
                info!(
                    "This node is the initializer for group {:?}, initializing Raft",
                    request.group_id
                );
                layer
                    .raft()
                    .initialize(raft_members)
                    .await
                    .map_err(|e| EventError::Internal(format!("Failed to initialize Raft: {e}")))?;
            } else {
                info!(
                    "This node is not the initializer for group {:?}, Raft will join existing cluster",
                    request.group_id
                );
            }
        } else {
            info!(
                "Group {:?} Raft is already initialized, skipping Raft initialization",
                request.group_id
            );
        }

        info!(
            "Successfully ensured group {:?} is initialized",
            request.group_id
        );
        Ok(())
    }
}
