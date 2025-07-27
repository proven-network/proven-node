//! Handler for CreateGroup command

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{error, info};

use crate::foundation::events::{Error as EventError, EventBus, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::CreateGroup;
use crate::services::group_consensus::types::{ConsensusLayers, States};
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_storage::{StorageAdaptor, StorageManager};
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager};
use proven_transport::Transport;

/// Handler for CreateGroup command
pub struct CreateGroupHandler<T, G, A, S>
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
}

impl<T, G, A, S> CreateGroupHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    pub fn new(
        node_id: NodeId,
        groups: ConsensusLayers<S>,
        group_states: States,
        network_manager: Arc<NetworkManager<T, G, A>>,
        storage_manager: Arc<StorageManager<S>>,
        topology_manager: Arc<TopologyManager<G>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            groups,
            group_states,
            network_manager,
            storage_manager,
            topology_manager,
            event_bus,
        }
    }
}

#[async_trait]
impl<T, G, A, S> RequestHandler<CreateGroup> for CreateGroupHandler<T, G, A, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: CreateGroup,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "CreateGroupHandler: Creating local group {:?} with members: {:?}",
            request.group_id, request.members
        );

        // Check if we're a member of this group
        if !request.members.contains(&self.node_id) {
            info!(
                "Not a member of group {:?}, skipping local creation",
                request.group_id
            );
            return Ok(());
        }

        // Check if group already exists
        let groups_guard = self.groups.read().await;
        if groups_guard.contains_key(&request.group_id) {
            info!("Group {:?} already exists locally", request.group_id);
            return Ok(());
        }
        drop(groups_guard);

        // Create the group consensus layer
        use crate::consensus::group::GroupConsensusLayer;
        use crate::foundation::create_group_state_access;
        use crate::services::group_consensus::adaptor::GroupNetworkFactory;
        use crate::services::group_consensus::callbacks::GroupConsensusCallbacksImpl;

        info!("Creating consensus layer for group {:?}", request.group_id);

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
            self.node_id.clone(),
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
                self.node_id.clone(),
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

        // Store the layer
        {
            let mut groups = self.groups.write().await;
            groups.insert(request.group_id, layer.clone());
        }

        // TODO: Start event monitoring for this group (requires routing table)

        // Get topology information for members
        let mut raft_members = std::collections::BTreeMap::new();
        for member_id in &request.members {
            if let Some(node) = self.topology_manager.get_node(member_id).await {
                raft_members.insert(member_id.clone(), node);
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
                "This node is not the initializer for group {:?}, waiting for leader",
                request.group_id
            );
        }

        info!("Successfully created group {:?}", request.group_id);
        Ok(())
    }
}
